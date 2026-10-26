"""Isolated Docker smoke checks; never uses deployment .env or live services.

Build soma-containered-api:verify and soma-containered-ingress:verify first.
The test creates a temporary PostgreSQL instance, bridge and echo backends, then
removes all of its containers/network. Only random loopback ports are published.
"""
from __future__ import annotations

import base64
import json
import subprocess
import time
import urllib.error
import urllib.request
import uuid


API_IMAGE = "soma-containered-api:verify"
INGRESS_IMAGE = "soma-containered-ingress:verify"
GATEWAY_IMAGE = "soma-containered-gateway:verify"


def docker(*args):
    result = subprocess.run(["docker", *args], check=True, capture_output=True, text=True)
    return result.stdout.strip()


def wait_for(check, seconds=60):
    end = time.monotonic() + seconds
    last = None
    while time.monotonic() < end:
        try:
            return check()
        except (subprocess.CalledProcessError, OSError, ValueError, AssertionError) as exc:
            last = exc
            time.sleep(0.5)
    raise RuntimeError(f"Smoke check timed out: {last}")


def port(container, internal):
    return docker("port", container, str(internal)).rsplit(":", 1)[1]


def main():
    prefix = "soma-verify-" + uuid.uuid4().hex[:10]
    network = prefix + "-net"
    containers = []
    common_env = {
        "SANDBOX_SERVICE_URL": "http://sandbox.invalid:8001",
        "PRIVATE_NETWORK_CIDRS": '["127.0.0.0/8","172.16.0.0/12"]',
        "TRUSTED_PROXY_CIDRS": '["172.16.0.0/12"]',
        "DEBUG": "false", "AWS_EC2_METADATA_DISABLED": "true",
        "DOCKERHUB_VISIBILITY_ENABLED": "false",
        "DOCKERHUB_TASK_SYNC_ENABLED": "false", "HF_DATASET_ENABLED": "false",
    }
    docker("network", "create", network)

    def run(name, image, *args, env=None, aliases=(), publish=(), detach=True):
        full_name = prefix + "-" + name
        # Cleanup happens in `finally`; retaining failed containers until then
        # makes startup failures observable through the diagnostic log block.
        cmd = ["run", "--name", full_name, "--network", network]
        if detach:
            cmd.append("-d")
        for alias in aliases:
            cmd.extend(["--network-alias", alias])
        for target in publish:
            cmd.extend(["-p", f"127.0.0.1::{target}"])
        for key, value in (env or {}).items():
            cmd.extend(["-e", f"{key}={value}"])
        containers.append(full_name)
        docker(*cmd, image, *args)
        return full_name

    try:
        database = run("postgres", "postgres:16-alpine", env={
            "POSTGRES_PASSWORD": "isolated-test-only", "POSTGRES_DB": "soma_test",
        }, aliases=("postgres",))
        wait_for(lambda: docker("exec", database, "pg_isready", "-U", "postgres"))
        dsn = "postgresql+asyncpg://postgres:isolated-test-only@postgres:5432/soma_test"
        env = {**common_env, "POSTGRES_DSN": dsn}
        schema_setup = '''
import asyncio
from app.core.config import settings
from soma_shared.db.session import close_db, init_db
async def main():
    try:
        await init_db(dsn=settings.get_postgres_writer_dsn(), echo=False, pool_size=1, max_overflow=0)
    finally:
        await close_db()
asyncio.run(main())
'''
        run("schema-setup", API_IMAGE, "python", "-c", schema_setup, env=env, detach=False)
        print("PASS: isolated test schema setup", flush=True)

        docker("exec", database, "psql", "-U", "postgres", "-d", "soma_test", "-v", "ON_ERROR_STOP=1", "-c",
               "CREATE ROLE runtime LOGIN PASSWORD 'runtime-test-only'; "
               "GRANT CONNECT ON DATABASE soma_test TO runtime; "
               "GRANT USAGE ON SCHEMA public TO runtime; "
               "GRANT SELECT ON ALL TABLES IN SCHEMA public TO runtime;")
        adapter_check = '''
import asyncio
from app.runtime.database import connect_existing_database
from soma_shared.db.session import get_db_session, close_db
from sqlalchemy import text
async def main():
    await connect_existing_database(dsn="postgresql+asyncpg://runtime:runtime-test-only@postgres:5432/soma_test", echo=False, pool_size=1, max_overflow=0)
    async for db in get_db_session():
        assert (await db.execute(text("select count(*) from competitions"))).scalar() == 0
        break
    await close_db()
asyncio.run(main())
'''
        run("adapter", API_IMAGE, "python", "-c", adapter_check, env=env, detach=False)
        print("PASS: connection-only adapter works with a non-owner SELECT-only role", flush=True)

        coordinator = run("coordinator", API_IMAGE, env={
            **env, "SOMA_SERVICE_ROLE": "coordinator", "DB_INITIALIZE_SCHEMA": "false",
        }, publish=(8000,))
        coordinator_url = f"http://127.0.0.1:{port(coordinator, 8000)}/health/ready"
        wait_for(lambda: urllib.request.urlopen(coordinator_url, timeout=2).read())
        print("PASS: real coordinator starts and is ready without wallet/chain credentials", flush=True)

        stub = r'''
from http.server import BaseHTTPRequestHandler, HTTPServer
import os, json, base64
class Handler(BaseHTTPRequestHandler):
    def do_GET(self): self.do_POST()
    def do_POST(self):
        body=self.rfile.read(int(self.headers.get("content-length",0)))
        if os.environ["STUB_NAME"] == "upstream":
            content=b'data: {"choices":[{"delta":{"content":"hello"}}]}\n\ndata: [DONE]\n\n'
            self.send_response(200);self.send_header("Content-Type","text/event-stream");self.send_header("Content-Length",str(len(content)));self.end_headers();self.wfile.write(content);return
        value={"service":os.environ["STUB_NAME"],"path":self.path,"body":base64.b64encode(body).decode(),"forwarded":self.headers.get("X-Forwarded-For"),"request_id":self.headers.get("X-Request-ID")}
        content=json.dumps(value).encode()
        self.send_response(200);self.send_header("Content-Type","application/json");self.send_header("Content-Length",str(len(content)));self.end_headers();self.wfile.write(content)
HTTPServer(("0.0.0.0",int(os.environ["STUB_PORT"])),Handler).serve_forever()
'''
        for name in ("platform-api", "run-coordinator", "results", "upstream"):
            run("stub-" + name, API_IMAGE, "python", "-u", "-c", stub,
                env={"STUB_NAME": name, "STUB_PORT": "8000"}, aliases=(name,))
        gateway_code = '''
import uvicorn
from gateway import main
async def auth(run_id): return "Bearer isolated-test", "smoke-miner"
main._resolve_authorization_header=auth
uvicorn.run(main.app,host="0.0.0.0",port=8050,proxy_headers=False)
'''
        gateway_one = run("gateway-1", GATEWAY_IMAGE, "python", "-c", gateway_code, env={
            "POSTGRES_DSN": dsn, "GATEWAY_UPSTREAM_BASE_URL": "http://upstream:8000/v1",
            "GATEWAY_INSTANCE_ID": "gateway-1",
        }, aliases=("gateway-1",))
        run("gateway-2", GATEWAY_IMAGE, "python", "-c", gateway_code, env={
            "POSTGRES_DSN": dsn, "GATEWAY_UPSTREAM_BASE_URL": "http://upstream:8000/v1",
            "GATEWAY_INSTANCE_ID": "gateway-2",
        }, aliases=("gateway-2",))
        ingress = run("ingress", INGRESS_IMAGE, publish=(8000, 8050))
        api_url = f"http://127.0.0.1:{port(ingress, 8000)}"
        wait_for(lambda: urllib.request.urlopen(api_url + "/health/live", timeout=2).read())
        body = b'{"payload": {"solution":"print(1)"}, "sig": {"nonce":"unchanged"}}'
        for path, expected in (
            ("/miner/upload-solution?x=one%20two", "platform-api"),
            ("/validator/get_best_miners", "platform-api"),
            ("/api/private/sandbox/compact-bench/report", "run-coordinator"),
            ("/api/private/frontend/competitions-list", "results"),
            ("/api/private/incentives/123/approve", "results"),
        ):
            request = urllib.request.Request(api_url + path, data=body, headers={
                "Content-Type": "application/json", "X-Forwarded-For": "10.1.2.3",
                "X-Request-ID": "smoke-request",
            })
            response = wait_for(lambda: json.load(urllib.request.urlopen(request, timeout=3)))
            assert response["service"] == expected, response
            assert response["path"] == path
            assert base64.b64decode(response["body"]) == body
            assert response["forwarded"] != "10.1.2.3"
            assert response["request_id"] == "smoke-request"
        print("PASS: Docker ingress preserves paths, query strings and signed bodies; rejects spoofed forwarding headers", flush=True)
        gateway_url = f"http://127.0.0.1:{port(ingress, 8050)}/v1/chat/completions"
        request = urllib.request.Request(gateway_url, data=b'{"model":"smoke-model","messages":[],"stream":true}', headers={
            "Content-Type": "application/json", "X-Run-Id": "123",
        })
        content = wait_for(lambda: urllib.request.urlopen(request, timeout=5).read())
        assert content == b'data: {"choices":[{"delta":{"content":"hello"}}]}\n\ndata: [DONE]\n\n', content
        control = '''
import json, urllib.request
request = urllib.request.Request("http://127.0.0.1:8050/_internal/gateway/drain", data=b"", method="POST")
print(urllib.request.urlopen(request, timeout=2).read().decode())
'''
        drained = json.loads(docker("exec", gateway_one, "python", "-c", control))
        assert drained["draining"] is True and drained["active_requests"] == 0, drained
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port(ingress, 8050)}/_internal/gateway/status", timeout=2)
            raise AssertionError("ingress exposed a gateway control route")
        except urllib.error.HTTPError as exc:
            assert exc.code == 404, exc
        # A drained member rejects before contacting a provider. The Nginx
        # internal retry is safe because the complete JSON request is buffered.
        for _ in range(4):
            content = wait_for(lambda: urllib.request.urlopen(request, timeout=5).read())
            assert content.endswith(b"data: [DONE]\n\n"), content
        print("PASS: gateway pool preserves SSE, hides control endpoints and drains a member safely", flush=True)
    except BaseException:
        for name in containers:
            result = subprocess.run(["docker", "logs", name], capture_output=True, text=True)
            output = (result.stdout + result.stderr).strip()
            if output:
                print(f"--- logs: {name} ---\n{output}", flush=True)
        raise
    finally:
        for name in reversed(containers):
            subprocess.run(["docker", "rm", "-f", name], capture_output=True)
        subprocess.run(["docker", "network", "rm", network], capture_output=True)


if __name__ == "__main__":
    main()

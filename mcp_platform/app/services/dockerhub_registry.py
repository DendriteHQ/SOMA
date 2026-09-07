"""Docker Hub client for the hidden-task image repository.

Two different APIs are involved and they are not interchangeable:

* ``hub.docker.com/v2`` owns repository *metadata* - existence, visibility, the tag
  listing, tag deletion. It authenticates with a JWT obtained by exchanging the
  personal access token, the same way the docker CLI does.
* ``registry-1.docker.io`` owns the *content* - manifests and blobs. It authenticates
  with a per-scope bearer token from ``auth.docker.io``.

Copying a task's images goes through the registry API rather than ``docker pull`` +
``docker push`` because of cross-repository blob mounts: both repositories live in the
same namespace, so the registry can relink the existing blobs instead of accepting an
upload. That turns a multi-GB dind image copy into a few seconds of manifest work, and
means the platform never has to have the image on disk at all.

``scripts/dockerhub_task_repo.py`` is the operator-facing CLI over this module. Keeping
one implementation matters most for :func:`set_private`, whose two workarounds are
described in its docstring - a second copy of that code drifting out of agreement would
publish hidden tasks.
"""

from __future__ import annotations

import base64
import json
import urllib.error
import urllib.request

from app.core.config import settings

HUB_API = "https://hub.docker.com/v2"
REGISTRY_API = "https://registry-1.docker.io"
AUTH_API = "https://auth.docker.io/token"

_LOGIN_PATH = "/users/login/"

# Manifest content transfers can move real bytes when a blob mount is refused, so they
# get their own generous timeout instead of the metadata one.
_REGISTRY_TIMEOUT_SECONDS = 1800.0

# Every manifest media type Docker Hub can answer a manifest GET with. Sent as the
# Accept header so a multi-arch tag comes back as its index instead of the registry
# picking an arbitrary child for us.
MANIFEST_MEDIA_TYPES = (
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.docker.distribution.manifest.v2+json",
)
INDEX_MEDIA_TYPES = frozenset(
    {
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
    }
)


class DockerHubError(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# hub.docker.com - repository metadata
# ---------------------------------------------------------------------------


def _credentials() -> tuple[str, str]:
    username = (settings.dockerhub_username or "").strip()
    token = (settings.dockerhub_token or "").strip()
    if not username or not token:
        raise DockerHubError(
            "DOCKERHUB_USERNAME and DOCKERHUB_TOKEN are required for visibility management"
        )
    return username, token


def hub_call(
    path: str,
    *,
    method: str = "GET",
    body: dict | None = None,
    jwt: str | None = None,
) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        f"{HUB_API}{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    if jwt:
        request.add_header("Authorization", f"Bearer {jwt}")
    timeout = float(settings.dockerhub_api_timeout_seconds)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise DockerHubError(f"{method} {path} failed ({exc.code}): {detail}") from exc
    except urllib.error.URLError as exc:
        raise DockerHubError(f"{method} {path} failed: {exc.reason}") from exc
    return json.loads(raw) if raw else {}


def login() -> str:
    username, token = _credentials()
    # A personal access token is exchanged for a short-lived JWT the same way the
    # docker CLI does it; the token itself is never sent to the repository endpoints.
    payload = hub_call(
        _LOGIN_PATH,
        method="POST",
        body={"username": username, "password": token},
    )
    jwt = str(payload.get("token") or "")
    if not jwt:
        raise DockerHubError("Docker Hub login returned no token")
    return jwt


def split_repository(repository: str) -> tuple[str, str]:
    if "/" not in repository:
        raise DockerHubError(
            f"repository must be '<namespace>/<name>', got {repository!r}"
        )
    namespace, name = repository.split("/", 1)
    return namespace.strip(), name.strip()


def is_private(repository: str, *, jwt: str) -> bool:
    namespace, name = split_repository(repository)
    payload = hub_call(f"/repositories/{namespace}/{name}/", jwt=jwt)
    return bool(payload.get("is_private"))


def set_private(repository: str, *, jwt: str, private: bool) -> bool:
    """Set the repository's visibility and return what it actually became.

    Visibility is changed through the dedicated ``privacy/`` endpoint, and the body
    must always carry an explicit ``is_private`` boolean. Both details were verified
    against the live API and both matter:

    * ``PATCH /v2/repositories/{ns}/{repo}/`` accepts an ``is_private`` field and
      echoes it back in its response, but does not apply it - it silently leaves the
      repository as it was. Trusting that response reports a change that never
      happened.
    * The ``privacy/`` endpoint treats a body without a recognised ``is_private`` key
      as a request to make the repository PUBLIC. A typo or a renamed field therefore
      fails in the one direction that leaks hidden tasks, which is why the value is
      sent as an explicit boolean and never as a status string.

    The endpoint returns an empty body either way, so the result is read back from the
    repository itself rather than inferred from the request having succeeded.
    """
    namespace, name = split_repository(repository)
    hub_call(
        f"/repositories/{namespace}/{name}/privacy/",
        method="POST",
        jwt=jwt,
        body={"is_private": bool(private)},
    )
    return is_private(repository, jwt=jwt)


def repository_exists(repository: str, *, jwt: str) -> bool:
    namespace, name = split_repository(repository)
    try:
        hub_call(f"/repositories/{namespace}/{name}/", jwt=jwt)
    except DockerHubError as exc:
        if "(404)" in str(exc):
            return False
        raise
    return True


def create_private_repository(repository: str, *, jwt: str, description: str = "") -> bool:
    """Create ``repository`` private. Returns False when it already existed.

    Created explicitly rather than letting the first manifest push create it: a Docker
    Hub repository created implicitly by a push is PUBLIC, which would publish the
    hidden task images the moment the first tag lands.
    """
    if repository_exists(repository, jwt=jwt):
        return False
    namespace, name = split_repository(repository)
    hub_call(
        "/repositories/",
        method="POST",
        jwt=jwt,
        body={
            "namespace": namespace,
            "name": name,
            "is_private": True,
            "description": description,
            "full_description": "",
        },
    )
    return True


def list_tags(repository: str, *, jwt: str) -> list[dict]:
    namespace, name = split_repository(repository)
    results: list[dict] = []
    path = f"/repositories/{namespace}/{name}/tags/?page_size=100"
    while path:
        payload = hub_call(path, jwt=jwt)
        results.extend(payload.get("results", []))
        next_url = payload.get("next")
        path = next_url.replace(HUB_API, "") if next_url else ""
    return results


def list_tag_names(repository: str, *, jwt: str) -> set[str]:
    return {
        str(tag.get("name") or "").strip()
        for tag in list_tags(repository, jwt=jwt)
        if str(tag.get("name") or "").strip()
    }


def delete_tag(repository: str, tag: str, *, jwt: str) -> None:
    """Remove one tag. A tag that is already gone is not an error."""
    namespace, name = split_repository(repository)
    try:
        hub_call(f"/repositories/{namespace}/{name}/tags/{tag}/", method="DELETE", jwt=jwt)
    except DockerHubError as exc:
        if "(404)" in str(exc):
            return
        raise


# ---------------------------------------------------------------------------
# registry-1.docker.io - manifests and blobs
# ---------------------------------------------------------------------------


def registry_token(scopes: list[str]) -> str:
    username, token = _credentials()
    query = "service=registry.docker.io&" + "&".join(f"scope={scope}" for scope in scopes)
    request = urllib.request.Request(f"{AUTH_API}?{query}")
    basic = base64.b64encode(f"{username}:{token}".encode()).decode("ascii")
    request.add_header("Authorization", f"Basic {basic}")
    try:
        with urllib.request.urlopen(
            request, timeout=float(settings.dockerhub_api_timeout_seconds)
        ) as response:
            return json.load(response)["token"]
    except urllib.error.HTTPError as exc:
        raise DockerHubError(
            f"registry token request failed ({exc.code}): "
            f"{exc.read().decode(errors='replace')}"
        ) from exc


def copy_scope_token(*, source: str, target: str) -> str:
    """A token that can read ``source`` and write ``target`` - what a copy needs."""
    return registry_token(
        [f"repository:{source}:pull", f"repository:{target}:pull,push"]
    )


def _registry_call(
    method: str,
    path: str,
    *,
    token: str,
    headers: dict[str, str] | None = None,
    data: bytes | None = None,
):
    request = urllib.request.Request(f"{REGISTRY_API}{path}", method=method, data=data)
    request.add_header("Authorization", f"Bearer {token}")
    for key, value in (headers or {}).items():
        request.add_header(key, value)
    return urllib.request.urlopen(request, timeout=_REGISTRY_TIMEOUT_SECONDS)


def get_manifest(repository: str, reference: str, *, token: str) -> tuple[bytes, str]:
    response = _registry_call(
        "GET",
        f"/v2/{repository}/manifests/{reference}",
        token=token,
        headers={"Accept": ", ".join(MANIFEST_MEDIA_TYPES)},
    )
    with response:
        return response.read(), response.headers.get("Content-Type", "")


def manifest_exists(repository: str, reference: str, *, token: str) -> bool:
    try:
        response = _registry_call(
            "HEAD",
            f"/v2/{repository}/manifests/{reference}",
            token=token,
            headers={"Accept": ", ".join(MANIFEST_MEDIA_TYPES)},
        )
    except urllib.error.HTTPError as exc:
        if exc.code in (404, 401):
            return False
        raise
    with response:
        return True


def blob_exists(repository: str, digest: str, *, token: str) -> bool:
    try:
        response = _registry_call("HEAD", f"/v2/{repository}/blobs/{digest}", token=token)
    except urllib.error.HTTPError as exc:
        if exc.code in (404, 401):
            return False
        raise
    with response:
        return True


def copy_blob(*, digest: str, source: str, target: str, token: str) -> str:
    """Make ``digest`` available in ``target``. Returns what it took to get there."""
    if blob_exists(target, digest, token=token):
        return "present"

    # Cross-repo mount: the registry relinks the existing blob instead of accepting an
    # upload. 201 means it did; 202 means it opened a normal upload session instead
    # (mount refused), which is the signal to fall back to streaming the bytes.
    try:
        response = _registry_call(
            "POST",
            f"/v2/{target}/blobs/uploads/?mount={digest}&from={source}",
            token=token,
            data=b"",
        )
    except urllib.error.HTTPError as exc:
        raise DockerHubError(
            f"blob mount for {digest} failed ({exc.code}): "
            f"{exc.read().decode(errors='replace')}"
        ) from exc
    with response:
        status = response.status
        upload_location = response.headers.get("Location", "")
    if status == 201:
        return "mounted"

    if not upload_location:
        raise DockerHubError(f"blob mount for {digest} returned no upload location")
    with _registry_call("GET", f"/v2/{source}/blobs/{digest}", token=token) as source_blob:
        payload = source_blob.read()
    separator = "&" if "?" in upload_location else "?"
    put_path = upload_location.replace(REGISTRY_API, "")
    with _registry_call(
        "PUT",
        f"{put_path}{separator}digest={digest}",
        token=token,
        headers={"Content-Type": "application/octet-stream"},
        data=payload,
    ):
        pass
    return f"uploaded ({len(payload)} bytes)"


def put_manifest(
    *,
    repository: str,
    reference: str,
    payload: bytes,
    media_type: str,
    token: str,
) -> None:
    try:
        with _registry_call(
            "PUT",
            f"/v2/{repository}/manifests/{reference}",
            token=token,
            headers={"Content-Type": media_type},
            data=payload,
        ):
            pass
    except urllib.error.HTTPError as exc:
        raise DockerHubError(
            f"manifest PUT {repository}:{reference} failed ({exc.code}): "
            f"{exc.read().decode(errors='replace')}"
        ) from exc


def copy_manifest(*, source: str, target: str, reference: str, token: str) -> str:
    """Copy one manifest (tag or digest) from ``source`` to ``target``.

    Indexes are copied children-first: an index whose children are not in the target
    repository yet is rejected by the registry, so each child image manifest (and its
    blobs) has to land before the index that references it.
    """
    payload, media_type = get_manifest(source, reference, token=token)
    document = json.loads(payload)
    media_type = media_type or str(document.get("mediaType", ""))

    if media_type in INDEX_MEDIA_TYPES:
        for child in document.get("manifests", []):
            child_digest = str(child.get("digest", ""))
            if not child_digest:
                continue
            copy_manifest(
                source=source, target=target, reference=child_digest, token=token
            )
    else:
        blob_digests = [str(document.get("config", {}).get("digest", ""))]
        blob_digests.extend(str(layer.get("digest", "")) for layer in document.get("layers", []))
        for digest in [d for d in blob_digests if d]:
            copy_blob(digest=digest, source=source, target=target, token=token)

    put_manifest(
        repository=target,
        reference=reference,
        payload=payload,
        media_type=media_type,
        token=token,
    )
    return media_type


def copy_tag(*, source: str, target: str, tag: str, token: str) -> str:
    """Copy one tag, reporting what happened rather than raising on a no-op.

    ``missing-in-source`` is a data problem (the task has no built image), not a
    transport failure, so it is returned for the caller to surface instead of aborting
    the rest of the batch.
    """
    if manifest_exists(target, tag, token=token):
        return "already-present"
    if not manifest_exists(source, tag, token=token):
        return "missing-in-source"
    copy_manifest(source=source, target=target, reference=tag, token=token)
    return "copied"

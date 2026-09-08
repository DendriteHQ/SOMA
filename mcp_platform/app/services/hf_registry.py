"""Hugging Face client for the hidden-task dataset repository.

Two hosts, as with Docker Hub, and they are not interchangeable:

* ``huggingface.co/api`` owns repository *metadata* - existence, visibility, the file
  tree, the commit listing - and *writes*, which go through a single commit endpoint
  that takes an NDJSON stream of file and deletion entries.
* ``huggingface.co/datasets/<repo>/resolve/<rev>/<path>`` serves *content*. It is a
  plain file download, so a published dataset needs no client library and no dataset
  viewer on the consumer side: the sandbox and the validator fetch one JSONL.

This is deliberately hand-rolled on top of ``urllib`` rather than ``huggingface_hub``.
The platform needs six endpoints, and the alternative pulls ``huggingface_hub`` (and,
for anything dataset-shaped, ``datasets`` and ``pyarrow``) into the API process for
them.

A note on what ``super_squash`` does and does not buy, verified against the live API:
it collapses the commit *listing* to a single commit, but a commit that existed before
the squash still serves its exact tree under ``resolve/<sha>/``. So squashing keeps a
published repository's history from being *enumerable*; it is not an erase. The
guarantee that a future competition's tasks are not in this repository comes from
never committing them (see ``hf_task_sync.load_desired_rows``), not from squashing them
away afterwards.
"""

from __future__ import annotations

import base64
import json
import urllib.error
import urllib.parse
import urllib.request

from app.core.config import settings

HF_API = "https://huggingface.co/api"
HF_HOST = "https://huggingface.co"

#: Everything here addresses dataset repositories; models and spaces use the same
#: endpoint shapes under a different prefix, which the platform has no use for.
REPO_TYPE_PATH = "datasets"

DEFAULT_REVISION = "main"


class HuggingFaceError(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# auth
# ---------------------------------------------------------------------------


def _token() -> str:
    token = (settings.huggingface_token or "").strip()
    if not token:
        raise HuggingFaceError("HUGGINGFACE_TOKEN is required for task dataset management")
    return token


def _timeout() -> float:
    return float(settings.hf_api_timeout_seconds)


def split_repository(repository: str) -> tuple[str, str]:
    """``namespace/name`` -> ``(namespace, name)``."""
    parts = str(repository or "").strip().strip("/").split("/")
    if len(parts) != 2 or not all(part.strip() for part in parts):
        raise HuggingFaceError(
            f"Expected a dataset repository as 'namespace/name', got {repository!r}"
        )
    return parts[0].strip(), parts[1].strip()


# ---------------------------------------------------------------------------
# huggingface.co/api - metadata and writes
# ---------------------------------------------------------------------------


def api_call(
    path: str,
    *,
    method: str = "GET",
    body: dict | None = None,
    ndjson: list[dict] | None = None,
) -> dict | list:
    """One request against the Hub API, returning the decoded JSON body.

    ``ndjson`` is the commit endpoint's payload shape - one JSON object per line,
    ``application/x-ndjson`` - and is mutually exclusive with ``body``.
    """
    if body is not None and ndjson is not None:
        raise HuggingFaceError("api_call takes either body or ndjson, not both")

    if ndjson is not None:
        data = "".join(json.dumps(entry) + "\n" for entry in ndjson).encode()
        content_type = "application/x-ndjson"
    elif body is not None:
        data = json.dumps(body).encode()
        content_type = "application/json"
    else:
        data, content_type = None, "application/json"

    request = urllib.request.Request(
        f"{HF_API}{path}",
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {_token()}",
            "Content-Type": content_type,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=_timeout()) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise HuggingFaceError(f"{method} {path} failed ({exc.code}): {detail}") from exc
    except urllib.error.URLError as exc:
        raise HuggingFaceError(f"{method} {path} failed: {exc.reason}") from exc
    return json.loads(raw) if raw else {}


def dataset_info(repository: str) -> dict:
    namespace, name = split_repository(repository)
    payload = api_call(f"/{REPO_TYPE_PATH}/{namespace}/{name}")
    if not isinstance(payload, dict):
        raise HuggingFaceError(f"Unexpected dataset info payload for {repository}")
    return payload


def repository_exists(repository: str) -> bool:
    try:
        dataset_info(repository)
    except HuggingFaceError as exc:
        if " (404)" in str(exc) or " (401)" in str(exc) or " (403)" in str(exc):
            return False
        raise
    return True


def is_private(repository: str) -> bool:
    return bool(dataset_info(repository).get("private"))


def set_private(repository: str, *, private: bool) -> bool:
    """Set the repository's visibility and report what it actually is afterwards.

    The returned value is re-read rather than assumed: "the request was accepted" and
    "the repository moved" are different claims, and for a repository holding hidden
    tasks only the second one is worth logging. The Docker Hub client makes the same
    distinction for the same reason.
    """
    namespace, name = split_repository(repository)
    api_call(
        f"/{REPO_TYPE_PATH}/{namespace}/{name}/settings",
        method="PUT",
        body={"private": bool(private)},
    )
    return is_private(repository)


def list_files(repository: str, *, revision: str = DEFAULT_REVISION) -> list[str]:
    namespace, name = split_repository(repository)
    payload = api_call(f"/{REPO_TYPE_PATH}/{namespace}/{name}/tree/{revision}")
    if not isinstance(payload, list):
        raise HuggingFaceError(f"Unexpected tree payload for {repository}@{revision}")
    return sorted(
        str(entry.get("path"))
        for entry in payload
        if isinstance(entry, dict) and entry.get("type") == "file" and entry.get("path")
    )


def commit_count(repository: str, *, revision: str = DEFAULT_REVISION) -> int:
    """How many commits the repository's listing walks back through.

    Used as the "is this history clean" check before publishing: see the module
    docstring for what a squash does and does not remove.
    """
    namespace, name = split_repository(repository)
    payload = api_call(f"/{REPO_TYPE_PATH}/{namespace}/{name}/commits/{revision}")
    return len(payload) if isinstance(payload, list) else 0


def commit(
    repository: str,
    *,
    files: dict[str, bytes],
    deleted: tuple[str, ...] = (),
    summary: str,
    revision: str = DEFAULT_REVISION,
) -> str:
    """Write files and deletions as one commit, returning its sha.

    Content is inlined base64. Files large enough to require LFS are refused by
    :func:`ensure_within_size_limit` before they get here, because the alternative is
    a rejected commit at the moment the competition's tasks were supposed to publish.
    """
    if not files and not deleted:
        raise HuggingFaceError("A commit needs at least one file or deletion")

    namespace, name = split_repository(repository)
    entries: list[dict] = [{"key": "header", "value": {"summary": summary}}]
    for path, content in sorted(files.items()):
        entries.append(
            {
                "key": "file",
                "value": {
                    "path": path,
                    "content": base64.b64encode(content).decode(),
                    "encoding": "base64",
                },
            }
        )
    for path in sorted(deleted):
        entries.append({"key": "deletedFile", "value": {"path": path}})

    payload = api_call(
        f"/{REPO_TYPE_PATH}/{namespace}/{name}/commit/{revision}",
        method="POST",
        ndjson=entries,
    )
    if not isinstance(payload, dict) or not payload.get("success"):
        raise HuggingFaceError(f"Commit to {repository} was not accepted: {payload}")
    return str(payload.get("commitOid") or "")


def super_squash(
    repository: str,
    *,
    summary: str,
    revision: str = DEFAULT_REVISION,
) -> str:
    """Collapse the branch's commit listing into a single commit, returning its sha."""
    namespace, name = split_repository(repository)
    payload = api_call(
        f"/{REPO_TYPE_PATH}/{namespace}/{name}/super-squash/{revision}",
        method="POST",
        body={"message": summary},
    )
    if not isinstance(payload, dict):
        raise HuggingFaceError(f"Unexpected super-squash payload for {repository}")
    return str(payload.get("commitId") or "")


def ensure_within_size_limit(path: str, content: bytes) -> None:
    limit = int(settings.hf_dataset_max_bytes)
    if limit > 0 and len(content) > limit:
        raise HuggingFaceError(
            f"{path} is {len(content)} bytes, over the {limit}-byte inline commit "
            "limit; publishing it needs LFS support, which this client does not have"
        )


# ---------------------------------------------------------------------------
# huggingface.co/datasets/<repo>/resolve - content
# ---------------------------------------------------------------------------


def download_file(
    repository: str,
    path: str,
    *,
    revision: str = DEFAULT_REVISION,
) -> bytes | None:
    """The file's bytes, or ``None`` when it is not in the repository at ``revision``.

    A missing file is a normal state here - it is what an empty dataset looks like -
    so it is reported as ``None`` rather than raised.
    """
    namespace, name = split_repository(repository)
    quoted = urllib.parse.quote(path.lstrip("/"))
    url = f"{HF_HOST}/{REPO_TYPE_PATH}/{namespace}/{name}/resolve/{revision}/{quoted}"
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {_token()}"})
    try:
        with urllib.request.urlopen(request, timeout=_timeout()) as response:
            return response.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        detail = exc.read().decode(errors="replace")
        raise HuggingFaceError(f"GET {url} failed ({exc.code}): {detail}") from exc
    except urllib.error.URLError as exc:
        raise HuggingFaceError(f"GET {url} failed: {exc.reason}") from exc

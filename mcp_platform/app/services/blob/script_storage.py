from __future__ import annotations

from app.services.blob.base import BlobStorage


class ScriptStorage:
    def __init__(
        self,
        blob_storage: BlobStorage,
        *,
        hot_prefix: str = "hot",
        archive_prefix: str = "archive",
    ) -> None:
        self._blob_storage = blob_storage
        self._hot_prefix = hot_prefix.strip("/")
        self._archive_prefix = archive_prefix.strip("/")

    def hot_key(
        self,
        miner_ss58: str,
        script_uuid: str,
        *,
        date_prefix: str | None = None,
    ) -> str:
        prefix = f"{self._hot_prefix}/miner_solutions/{miner_ss58}"
        if date_prefix:
            prefix = f"{prefix}/{date_prefix}"
        return f"{prefix}/{script_uuid}.py"

    def archive_key(
        self,
        miner_ss58: str,
        script_uuid: str,
        *,
        date_prefix: str | None = None,
    ) -> str:
        prefix = self._archive_prefix
        if date_prefix:
            prefix = f"{prefix}/{date_prefix}"
        return f"{prefix}/{miner_ss58}/{script_uuid}.py"

    async def put_hot_script(
        self,
        miner_ss58: str,
        script_uuid: str,
        script: str,
        *,
        date_prefix: str | None = None,
    ) -> str:
        key = self.hot_key(miner_ss58, script_uuid, date_prefix=date_prefix)
        await self._blob_storage.put_bytes(
            key,
            script.encode("utf-8"),
            content_type="text/x-python",
        )
        return key

    async def get_hot_script(
        self,
        miner_ss58: str,
        script_uuid: str,
        *,
        date_prefix: str | None = None,
    ) -> str:
        key = self.hot_key(miner_ss58, script_uuid, date_prefix=date_prefix)
        data = await self._blob_storage.get_bytes(key)
        return data.decode("utf-8")

    async def put_archive_script(
        self,
        miner_ss58: str,
        script_uuid: str,
        script: str,
        *,
        date_prefix: str | None = None,
    ) -> str:
        key = self.archive_key(miner_ss58, script_uuid, date_prefix=date_prefix)
        await self._blob_storage.put_bytes(
            key,
            script.encode("utf-8"),
            content_type="text/x-python",
        )
        return key

    async def get_archive_script(
        self,
        miner_ss58: str,
        script_uuid: str,
        *,
        date_prefix: str | None = None,
    ) -> str:
        key = self.archive_key(miner_ss58, script_uuid, date_prefix=date_prefix)
        data = await self._blob_storage.get_bytes(key)
        return data.decode("utf-8")

    async def delete(self, key: str) -> None:
        await self._blob_storage.delete(key)

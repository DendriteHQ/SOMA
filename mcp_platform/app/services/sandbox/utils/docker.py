from typing import Optional
import logging

try:
    import docker  # type: ignore
    from docker.errors import APIError, NotFound, ImageNotFound  # type: ignore
except Exception:  # pragma: no cover
    docker = None  # type: ignore
    APIError = Exception  # type: ignore
    NotFound = Exception  # type: ignore
    ImageNotFound = Exception  # type: ignore


class DockerManager:
    def __init__(self):
        super().__init__()
        # docker SDK is optional at import-time (tests may not have it installed).
        self.client: Optional[object] = None

    def initialize_docker_client(self) -> None:
        """
        Initialize docker client connection.
        """
        if docker is None:
            raise RuntimeError("docker python package is not available")
        if self.client is None:
            self.client = docker.from_env()
        self.client.ping()

    def create_docker_internal_network(self, network_name: str) -> None:
        """
        Create a docker internal network for sandboxes to use.
        """
        if self.client is None:
            self.initialize_docker_client()
        try:
            self.client.networks.get(network_name)
            return
        except NotFound:
            pass
        self.client.networks.create(
            name=network_name,
            internal=True,
            check_duplicate=True,
        )

    def connect_sandbox_to_network(self, sandbox_id: str, network_name: str) -> None:
        """
        Connect a sandbox to a given docker network.
        """
        if self.client is None:
            self.initialize_docker_client()
        network = self.client.networks.get(network_name)
        container = self.client.containers.get(sandbox_id)
        try:
            network.connect(container)
        except APIError as exc:
            if "already exists" in str(exc).lower():
                return
            raise

    def cleanup_containers_by_name_prefix(self, prefix: str) -> int:
        """
        Remove all containers whose name matches the given prefix.
        """
        if docker is None:
            raise RuntimeError("docker python package is not available")
        if self.client is None:
            self.initialize_docker_client()
        removed = 0
        try:
            containers = self.client.containers.list(all=True, filters={"name": prefix})
        except Exception as exc:
            logging.warning(f"Failed to list containers for cleanup: {exc}")
            return removed
        for container in containers:
            try:
                container.remove(force=True)
                removed += 1
            except Exception as exc:
                logging.warning(f"Failed to remove container {container.name}: {exc}")
        return removed

    def ensure_image(
        self,
        image: str,
        *,
        build_context: str,
        dockerfile: str = "Dockerfile",
    ) -> bool:
        """
        Ensure a docker image exists locally; build it if missing.
        """
        if docker is None:
            raise RuntimeError("docker python package is not available")
        if self.client is None:
            self.initialize_docker_client()
        try:
            self.client.images.get(image)
            return True
        except ImageNotFound:
            pass
        except Exception as exc:
            logging.warning(f"Failed to check docker image {image}: {exc}")

        try:
            self.client.images.build(
                path=build_context, dockerfile=dockerfile, tag=image
            )
            logging.info(f"Built docker image {image} from {build_context}")
            return True
        except Exception as exc:
            logging.error(f"Failed to build docker image {image}: {exc}")
            return False

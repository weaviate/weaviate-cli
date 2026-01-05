from urllib.parse import urlparse

import click
import json
import os
import sys
import socket
import weaviate
from weaviate.exceptions import WeaviateGRPCUnavailableError
from pathlib import Path
from typing import Dict, Optional, Union
from weaviate.config import AdditionalConfig, Timeout as WeaviateTimeout


class ConfigManager:
    """
    Weaviate CLI config manager for handling configuration files.
    """

    default_file_path: str = ".config/weaviate/"
    default_file_name: str = "config.json"
    default_host: str = "localhost"
    default_port: int = 8080
    default_grpc_port: int = 50051

    def __init__(
        self, config_file: Optional[str] = None, user: Optional[str] = None
    ) -> None:
        """Initialize config manager with optional config file path"""
        self.user = user
        if config_file:
            assert os.path.isfile(
                config_file
            ), f"Config file '{config_file}' does not exist!"
            self.config_path: Union[str, Path] = config_file
            with open(self.config_path, "r", encoding="utf-8") as config_data:
                try:
                    self.config: Dict[str, Union[str, Dict[str, str]]] = json.load(
                        config_data
                    )
                except:
                    click.echo("Fatal Error: Config file is not valid JSON!")
                    sys.exit(1)
        else:
            self.config_path = Path(
                os.path.join(
                    os.getenv("HOME"), self.default_file_path, self.default_file_name
                )
            )

            if self.config_path.exists():
                with open(self.config_path, "r", encoding="utf-8") as config_file:
                    self.config = json.load(config_file)
            else:
                self.create_default_config()

    def create_default_config(self) -> None:
        """Create a new configuration file"""
        self.config = {
            "host": f"{self.default_host}",
            "http_port": f"{self.default_port}",
            "grpc_port": f"{self.default_grpc_port}",
        }

    def __check_host_docker_internal(self, port: int = 8080) -> bool:
        """Check if host.docker.internal is reachable."""
        try:
            # Attempt to connect to host.docker.internal on any port (e.g., 80)
            fd = socket.create_connection(("host.docker.internal", port), timeout=2)
            fd.close()  # Close the socket
            return True
        except (socket.timeout, socket.error):
            return False

    def __get_host(self, port: int = 8080) -> str:
        """Determine the appropriate host based on the environment."""
        if self.__check_host_docker_internal(port):
            return "host.docker.internal"  # macOS/Windows Docker
        else:
            return "localhost"  # Default fallback (Linux or unreachable)

    def __get_port(self, url_parsed):
        if url_parsed.port:
            return url_parsed.port
        if url_parsed.scheme == "https":
            return 443
        else:
            return 80

    def __is_url(self, host: str) -> bool:
        """Check if the host is a URL (has a scheme and a network location)."""
        try:
            result = urlparse(host)
            return all([result.scheme, result.netloc])
        except ValueError:
            return False

    def get_client(self) -> weaviate.WeaviateClient:
        """Get synchronous weaviate client from config"""
        return self._create_client(async_client=False)

    def get_async_client(self) -> weaviate.WeaviateAsyncClient:
        """Get asynchronous weaviate client from config"""
        return self._create_client(async_client=True)

    def _create_client(
        self, async_client: bool
    ) -> Union[weaviate.WeaviateClient, weaviate.WeaviateAsyncClient]:
        """Internal method to create client based on async flag"""
        auth_config: Optional[weaviate.auth.AuthCredentials] = None
        additional_config: Optional[AdditionalConfig] = (
            self.__additional_config_if_slow()
        )

        if "auth" in self.config:
            if self.config["auth"].get("type") == "user":
                if not self.user:
                    raise Exception("User must be specified when auth type is 'user'")
                if self.user not in self.config["auth"]:
                    raise Exception(f"User '{self.user}' not found in config file")
                auth_config = weaviate.auth.AuthApiKey(
                    api_key=self.config["auth"][self.user]
                )
            elif self.config["auth"].get("type") == "api_key":
                auth_config = weaviate.auth.AuthApiKey(
                    api_key=self.config["auth"]["api_key"]
                )

        # Shared headers (may be None)
        headers = self.config["headers"] if "headers" in self.config else None

        # Localhost handling uses special helpers
        if self.config["host"] == "localhost":
            common_kwargs = dict(
                host=self.__get_host(self.config["http_port"]),
                port=self.config["http_port"],
                grpc_port=self.config["grpc_port"],
                auth_credentials=auth_config,
                headers=headers,
                additional_config=additional_config,
            )
            if async_client:
                return weaviate.use_async_with_local(**common_kwargs)
            return weaviate.connect_to_local(**common_kwargs)

        # Weaviate Cloud handling with retry fallback
        if self.config["host"].endswith("weaviate.cloud"):
            common_kwargs = dict(
                cluster_url=self.config["host"],
                auth_credentials=auth_config,
                headers=headers,
                additional_config=additional_config,
            )
            if async_client:
                try:
                    return weaviate.use_async_with_weaviate_cloud(**common_kwargs)
                except WeaviateGRPCUnavailableError as e:
                    click.echo(
                        f"GRPC connection seems to be unavailable, re-connecting skipping checks: {e}"
                    )
                    return weaviate.use_async_with_weaviate_cloud(
                        **{**common_kwargs, "skip_init_checks": True}
                    )
            else:
                try:
                    return weaviate.connect_to_weaviate_cloud(**common_kwargs)
                except WeaviateGRPCUnavailableError as e:
                    click.echo(
                        f"GRPC connection seems to be unavailable, re-connecting skipping checks: {e}"
                    )
                    return weaviate.connect_to_weaviate_cloud(
                        **{**common_kwargs, "skip_init_checks": True}
                    )

        # Custom host handling (URL vs hostname/IP)
        if self.__is_url(self.config["host"]):
            host_parsed = urlparse(self.config["host"])
            grpc_parsed = urlparse(self.config["grpc_host"])
            http_port = (
                self.__get_port(host_parsed)
                if "http_port" not in self.config or self.config["http_port"] == ""
                else self.config["http_port"]
            )
            grpc_port = (
                self.__get_port(grpc_parsed)
                if "grpc_port" not in self.config or self.config["grpc_port"] == ""
                else self.config["grpc_port"]
            )
            common_kwargs = dict(
                http_host=host_parsed.hostname,
                grpc_host=grpc_parsed.hostname,
                grpc_secure=grpc_parsed.scheme == "https",
                http_secure=host_parsed.scheme == "https",
                http_port=http_port,
                grpc_port=grpc_port,
                auth_credentials=auth_config,
                headers=headers,
                additional_config=additional_config,
            )
        else:
            common_kwargs = dict(
                http_host=self.config["host"],
                grpc_host=self.config["grpc_host"],
                grpc_secure=self.config["grpc_port"] == 443,
                http_secure=self.config["http_port"] == 443,
                http_port=self.config["http_port"],
                grpc_port=self.config["grpc_port"],
                auth_credentials=auth_config,
                headers=headers,
                additional_config=additional_config,
            )

        if async_client:
            return weaviate.use_async_with_custom(**common_kwargs)
        return weaviate.connect_to_custom(**common_kwargs)

    def __additional_config_if_slow(self) -> Optional[AdditionalConfig]:
        """Return AdditionalConfig with doubled timeouts when SLOW_CONNECTION is set."""
        env_flag = os.getenv("SLOW_CONNECTION")
        if not env_flag:
            return None
        if str(env_flag).strip().lower() not in {"1", "true", "yes", "on"}:
            return None
        defaults = AdditionalConfig().timeout
        return AdditionalConfig(
            timeout=WeaviateTimeout(
                query=defaults.query * 2,
                insert=defaults.insert * 2,
                init=defaults.init * 2,
            )
        )

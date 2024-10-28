import click
import json
import os
import sys
import socket
import weaviate
from pathlib import Path




class ConfigManager:
    """
    Weaviate CLI config manager for handling configuration files.
    """
    default_file_path = ".config/weaviate/"
    default_file_name = "config.json"
    default_host = "localhost"
    default_port = 8080
    default_grpc_port = 50051

    def __init__(self, config_file: str = None):
        """Initialize config manager with optional config file path"""
        if config_file:
            assert os.path.isfile(config_file), "Config file does not exist!"
            self.config_path = config_file
            with open(self.config_path, 'r', encoding="utf-8") as config_data:
                try:
                    self.config = json.load(config_data)
                except:
                    click.echo("Fatal Error: Config file is not valid JSON!")
                    sys.exit(1)
        else:
            self.config_path = Path(os.path.join(os.getenv("HOME"),
                                            self.default_file_path,
                                            self.default_file_name))
            self.config_path.parent.mkdir(parents=True, exist_ok=True)
            self.config_path.touch(exist_ok=True)
            with open(self.config_path, 'r', encoding="utf-8") as config_file:
                try:
                    self.config = json.load(config_file)
                except Exception as e:
                    click.echo("No existing configuration found, creating new one.")
                    self.create_new_config()

    def create_new_config(self):
        """Create a new configuration file"""
        self.config = {
            "url": f"{self.default_host}",
            "http_port": f"{self.default_port}",
            "grpc_port": f"{self.default_grpc_port}"
        }

    def __check_host_docker_internal(self, port: int = 8080):
        """Check if host.docker.internal is reachable."""
        try:
            # Attempt to connect to host.docker.internal on any port (e.g., 80)
            fd = socket.create_connection(("host.docker.internal", port), timeout=2)
            fd.close()  # Close the socket
            return True
        except (socket.timeout, socket.error):
            return False


    def __get_host(self, port: int = 8080):
        """Determine the appropriate host based on the environment."""
        if self.__check_host_docker_internal(port):
            return "host.docker.internal"  # macOS/Windows Docker
        else:
            return "localhost"  # Default fallback (Linux or unreachable)
    
    def get_client(self):
        """Get weaviate client from config"""
        
        auth_config = None
        if hasattr(self.config, "auth"):
            if self.config["auth"]["type"] == "api_key":
                auth_config = weaviate.auth.AuthApiKey(api_key=self.config["auth"]["api_key"])
            # elif self.config["auth"]["type"] == "client_secret":
            #     # auth_config = self.config["auth"]["secret"]
            # elif self.config["auth"]["type"] == "username_pass":
                # auth_config = weaviate.auth.AuthClientP(self.config["auth"]["user"], self.config["auth"]["pass"])
            # else:
            # click.echo("Fatal Error: Unknown authentication type in config!")
            # sys.exit(1)
                
        if self.config["host"] == "localhost":
            return weaviate.connect_to_local(
                host=self.__get_host(self.config["http_port"]),
                port=self.config["http_port"],
                grpc_port=self.config["grpc_port"],
                auth_credentials=auth_config
            )
        else:
            return weaviate.connect_to_wcs(
                cluster_url=self.config["url"],
                auth_credentials=auth_config
            )



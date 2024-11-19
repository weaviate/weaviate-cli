import socket
import pytest
import weaviate
from unittest.mock import MagicMock, mock_open, patch
from weaviate_cli.managers.config_manager import ConfigManager
import json


def test_init_with_config_file():
    config_data = {"host": "localhost", "http_port": "8080", "grpc_port": "50051"}

    with patch("builtins.open", mock_open(read_data=json.dumps(config_data))):
        with patch("os.path.isfile") as mock_isfile:
            mock_isfile.return_value = True
            config = ConfigManager(config_file="test_config.json")
            assert config.config == config_data


def test_create_default_config():
    with patch("pathlib.Path.exists") as mock_exists:
        mock_exists.return_value = False
        config = ConfigManager()
        assert config.config["host"] == "localhost"
        assert config.config["http_port"] == "8080"
        assert config.config["grpc_port"] == "50051"


@patch("socket.create_connection")
def test_check_host_docker_internal(mock_socket):
    config = ConfigManager()

    # Test successful connection
    mock_socket.return_value = MagicMock()
    assert config._ConfigManager__check_host_docker_internal() == True


@patch("socket.create_connection")
def test_check_host_docker_internal_failed(mock_socket):
    config = ConfigManager()

    # Test failed connection
    mock_socket.side_effect = socket.error()
    assert config._ConfigManager__check_host_docker_internal() == False


def test_init_with_user():
    config_data = {
        "host": "localhost",
        "http_port": "8080",
        "grpc_port": "50051",
        "auth": {"type": "user", "admin": "admin-key", "jose": "jose-key"},
    }

    with patch("builtins.open", mock_open(read_data=json.dumps(config_data))):
        with patch("os.path.isfile") as mock_isfile:
            mock_isfile.return_value = True
            config = ConfigManager(config_file="test_config.json", user="jose")
            assert config.user == "jose"
            assert config.config == config_data


def test_get_client_with_user_auth():
    config_data = {
        "host": "localhost",
        "http_port": "8080",
        "grpc_port": "50051",
        "auth": {"type": "user", "admin": "admin-key", "jose": "jose-key"},
    }

    with patch("builtins.open", mock_open(read_data=json.dumps(config_data))):
        with patch("os.path.isfile") as mock_isfile:
            mock_isfile.return_value = True
            with patch("weaviate.connect_to_local") as mock_connect:
                config = ConfigManager(config_file="test_config.json", user="jose")
                config.get_client()

                mock_connect.assert_called_once()
                call_kwargs = mock_connect.call_args.kwargs
                assert isinstance(
                    call_kwargs["auth_credentials"], weaviate.auth.AuthApiKey
                )
                assert call_kwargs["auth_credentials"].api_key == "jose-key"


def test_get_client_with_invalid_user():
    config_data = {
        "host": "localhost",
        "http_port": "8080",
        "grpc_port": "50051",
        "auth": {"type": "user", "admin": "admin-key"},
    }

    with patch("builtins.open", mock_open(read_data=json.dumps(config_data))):
        with patch("os.path.isfile") as mock_isfile:
            mock_isfile.return_value = True
            config = ConfigManager(config_file="test_config.json", user="jose")
            with pytest.raises(Exception) as exc_info:
                config.get_client()
            assert str(exc_info.value) == "User 'jose' not found in config file"


def test_get_client_missing_user():
    config_data = {
        "host": "localhost",
        "http_port": "8080",
        "grpc_port": "50051",
        "auth": {"type": "user", "admin": "admin-key"},
    }

    with patch("builtins.open", mock_open(read_data=json.dumps(config_data))):
        with patch("os.path.isfile") as mock_isfile:
            mock_isfile.return_value = True
            config = ConfigManager(config_file="test_config.json")
            with pytest.raises(Exception) as exc_info:
                config.get_client()
            assert (
                str(exc_info.value) == "User must be specified when auth type is 'user'"
            )


def test_get_client_with_api_key_auth():
    config_data = {
        "host": "localhost",
        "http_port": "8080",
        "grpc_port": "50051",
        "auth": {"type": "api_key", "api_key": "test-key"},
    }

    with patch("builtins.open", mock_open(read_data=json.dumps(config_data))):
        with patch("os.path.isfile") as mock_isfile:
            mock_isfile.return_value = True
            with patch("weaviate.connect_to_local") as mock_connect:
                config = ConfigManager(config_file="test_config.json")
                config.get_client()

                mock_connect.assert_called_once()
                call_kwargs = mock_connect.call_args.kwargs
                assert isinstance(
                    call_kwargs["auth_credentials"], weaviate.auth.AuthApiKey
                )
                assert call_kwargs["auth_credentials"].api_key == "test-key"

import json
import pytest
from click.testing import CliRunner
from cli import main
from unittest.mock import patch, MagicMock


@pytest.fixture
def cli_runner():
    return CliRunner()


def test_main_without_config_file(cli_runner):
    result = cli_runner.invoke(main)
    assert result.exit_code == 0


def test_main_with_non_existing_config_file(cli_runner):
    with patch("weaviate_cli.managers.config_manager.ConfigManager") as mock_config:
        result = cli_runner.invoke(
            main, ["--config-file", "test_config.json", "get", "shards"]
        )
        # Non existing config file is a usage error, so error code 2 is expected
        assert result.exit_code == 2
        assert (
            "Error: Invalid value for '--config-file': Path 'test_config.json' does not exist"
            in result.output
        )
        mock_config.assert_not_called()


def test_main_with_invalid_command(cli_runner):
    result = cli_runner.invoke(main, ["invalid_command"])
    assert result.exit_code == 2
    assert "No such command 'invalid_command'" in result.output


def test_main_help(cli_runner):
    result = cli_runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "Usage:" in result.output
    assert "--config-file" in result.output


def test_main_commands_registered():
    # Test that all commands are properly registered
    assert "create" in main.commands
    assert "delete" in main.commands
    assert "get" in main.commands
    assert "update" in main.commands
    assert "restore" in main.commands
    assert "query" in main.commands

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


def test_update_namespace_registered():
    assert "namespace" in main.commands["update"].commands


def test_create_namespace_forwards_home_node(cli_runner):
    with (
        patch(
            "weaviate_cli.commands.create.get_client_from_context",
            return_value=MagicMock(),
        ),
        patch("weaviate_cli.commands.create.NamespaceManager") as mock_manager_cls,
    ):
        result = cli_runner.invoke(
            main,
            ["create", "namespace", "--name", "tenantswest", "--home_node", "node1"],
        )
    assert result.exit_code == 0
    mock_manager_cls.return_value.create_namespace.assert_called_once_with(
        name="tenantswest", home_node="node1", json_output=False
    )


def test_update_namespace_forwards_home_node(cli_runner):
    with (
        patch(
            "weaviate_cli.commands.update.get_client_from_context",
            return_value=MagicMock(),
        ),
        patch("weaviate_cli.commands.update.NamespaceManager") as mock_manager_cls,
    ):
        result = cli_runner.invoke(
            main,
            ["update", "namespace", "--name", "tenantswest", "--home_node", "node2"],
        )
    assert result.exit_code == 0
    mock_manager_cls.return_value.update_namespace.assert_called_once_with(
        name="tenantswest", home_node="node2", json_output=False
    )


def test_update_namespace_requires_home_node(cli_runner):
    result = cli_runner.invoke(main, ["update", "namespace", "--name", "tenantswest"])
    # Missing required --home_node is a usage error.
    assert result.exit_code == 2
    assert "--home_node" in result.output


def test_create_user_forwards_qualified_user_name(cli_runner):
    # A namespace-scoped user is created by passing a namespace-qualified id
    # ("<namespace>:<user>") as --user_name; the CLI forwards it verbatim.
    with (
        patch(
            "weaviate_cli.commands.create.get_client_from_context",
            return_value=MagicMock(),
        ),
        patch("weaviate_cli.commands.create.UserManager") as mock_manager_cls,
    ):
        mock_manager_cls.return_value.create_user.return_value = "api-key"
        result = cli_runner.invoke(
            main, ["create", "user", "--user_name", "tenantswest:scoped"]
        )
    assert result.exit_code == 0
    mock_manager_cls.return_value.create_user.assert_called_once_with(
        user_name="tenantswest:scoped"
    )


def test_create_user_rejects_namespace_option(cli_runner):
    # The dropped --namespace flag must no longer be accepted (usage error).
    result = cli_runner.invoke(
        main, ["create", "user", "--user_name", "scoped", "--namespace", "tenantswest"]
    )
    assert result.exit_code == 2
    assert "--namespace" in result.output

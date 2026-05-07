"""
End-to-end CLI regression tests for the bool / Optional contract.

Several methods of the weaviate-python-client are *not* the usual "raise on
failure" style. Instead, they accept multiple HTTP status codes as "ok" and
return:

  * ``bool``         -- ``False`` means a logical no-op (404 not found, 409
                        already in target state, ...)
  * ``Optional[X]``  -- ``None`` means "not found" (404)

The CLI must surface those as a non-zero exit code with a clear ``Error:``
message -- never silently report success. These tests lock in that contract
end-to-end (CliRunner -> command -> manager -> mocked client method) so the
bug class cannot creep back in.

If you add a new caller of one of these client APIs, add a test here.
"""

import pytest
from click.testing import CliRunner
from unittest.mock import MagicMock, patch

from cli import main


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def fake_client() -> MagicMock:
    """A MagicMock standing in for a real WeaviateClient."""
    return MagicMock()


def _invoke(cli_runner: CliRunner, fake_client: MagicMock, command_module: str, args):
    """Invoke ``main`` with ``args`` while patching
    ``get_client_from_context`` in the targeted command module so the CLI gets
    our fake client instead of opening a real connection."""
    target = f"{command_module}.get_client_from_context"
    with patch(target, return_value=fake_client):
        return cli_runner.invoke(main, args)


# ---------------------------------------------------------------------------
# delete user -- client.users.db.delete() returns False on 404
# ---------------------------------------------------------------------------


def test_cli_delete_user_not_found_exits_nonzero(cli_runner, fake_client):
    fake_client.users.db.delete.return_value = False

    result = _invoke(
        cli_runner,
        fake_client,
        "weaviate_cli.commands.delete",
        ["delete", "user", "--user_name", "ghost"],
    )

    assert result.exit_code == 1, result.output
    assert "User 'ghost' not found." in result.output
    assert "deleted successfully" not in result.output


def test_cli_delete_user_success(cli_runner, fake_client):
    fake_client.users.db.delete.return_value = True

    result = _invoke(
        cli_runner,
        fake_client,
        "weaviate_cli.commands.delete",
        ["delete", "user", "--user_name", "alice"],
    )

    assert result.exit_code == 0, result.output
    assert "alice" in result.output
    assert "deleted successfully" in result.output


# ---------------------------------------------------------------------------
# update user --activate / --deactivate -- bool=False on 409
# ---------------------------------------------------------------------------


def test_cli_update_user_activate_already_active_exits_nonzero(cli_runner, fake_client):
    fake_client.users.db.activate.return_value = False

    result = _invoke(
        cli_runner,
        fake_client,
        "weaviate_cli.commands.update",
        ["update", "user", "--user_name", "alice", "--activate"],
    )

    assert result.exit_code == 1, result.output
    assert "already active" in result.output
    assert "activated successfully" not in result.output


def test_cli_update_user_activate_success(cli_runner, fake_client):
    fake_client.users.db.activate.return_value = True

    result = _invoke(
        cli_runner,
        fake_client,
        "weaviate_cli.commands.update",
        ["update", "user", "--user_name", "alice", "--activate"],
    )

    assert result.exit_code == 0, result.output
    assert "activated successfully" in result.output


def test_cli_update_user_deactivate_already_deactivated_exits_nonzero(
    cli_runner, fake_client
):
    fake_client.users.db.deactivate.return_value = False

    result = _invoke(
        cli_runner,
        fake_client,
        "weaviate_cli.commands.update",
        ["update", "user", "--user_name", "alice", "--deactivate"],
    )

    assert result.exit_code == 1, result.output
    assert "already deactivated" in result.output
    assert "deactivated successfully" not in result.output


def test_cli_update_user_deactivate_success(cli_runner, fake_client):
    fake_client.users.db.deactivate.return_value = True

    result = _invoke(
        cli_runner,
        fake_client,
        "weaviate_cli.commands.update",
        ["update", "user", "--user_name", "alice", "--deactivate"],
    )

    assert result.exit_code == 0, result.output
    assert "deactivated successfully" in result.output


# ---------------------------------------------------------------------------
# get user -- client.users.db.get() returns None on 404
# ---------------------------------------------------------------------------


def test_cli_get_user_not_found_exits_nonzero(cli_runner, fake_client):
    fake_client.users.db.get.return_value = None

    result = _invoke(
        cli_runner,
        fake_client,
        "weaviate_cli.commands.get",
        ["get", "user", "--user_name", "ghost"],
    )

    assert result.exit_code == 1, result.output
    assert "User 'ghost' not found." in result.output


# ---------------------------------------------------------------------------
# delete alias -- client.alias.delete() returns False on 404
# ---------------------------------------------------------------------------


def test_cli_delete_alias_not_found_exits_nonzero(cli_runner, fake_client):
    fake_client.alias.delete.return_value = False

    result = _invoke(
        cli_runner,
        fake_client,
        "weaviate_cli.commands.delete",
        ["delete", "alias", "ghost_alias"],
    )

    assert result.exit_code == 1, result.output
    assert "Alias 'ghost_alias' not found." in result.output
    assert "deleted successfully" not in result.output


def test_cli_delete_alias_success(cli_runner, fake_client):
    fake_client.alias.delete.return_value = True

    result = _invoke(
        cli_runner,
        fake_client,
        "weaviate_cli.commands.delete",
        ["delete", "alias", "my_alias"],
    )

    assert result.exit_code == 0, result.output
    assert "deleted successfully" in result.output


# ---------------------------------------------------------------------------
# update alias -- client.alias.update() returns False on 404
# ---------------------------------------------------------------------------


def test_cli_update_alias_not_found_exits_nonzero(cli_runner, fake_client):
    fake_client.alias.update.return_value = False

    result = _invoke(
        cli_runner,
        fake_client,
        "weaviate_cli.commands.update",
        ["update", "alias", "ghost", "MyCollection"],
    )

    assert result.exit_code == 1, result.output
    assert "Alias 'ghost' not found." in result.output
    assert "updated successfully" not in result.output


def test_cli_update_alias_success(cli_runner, fake_client):
    fake_client.alias.update.return_value = True

    result = _invoke(
        cli_runner,
        fake_client,
        "weaviate_cli.commands.update",
        ["update", "alias", "my_alias", "MyCollection"],
    )

    assert result.exit_code == 0, result.output
    assert "updated successfully" in result.output


# ---------------------------------------------------------------------------
# delete data --uuid -- collection.data.delete_by_id() returns False on 404
# ---------------------------------------------------------------------------


def test_cli_delete_data_by_uuid_not_found_exits_nonzero(cli_runner, fake_client):
    # Pretend the collection exists and is not multi-tenant.
    fake_client.collections.exists.return_value = True
    mock_collection = MagicMock()
    mock_collection.name = "TestCollection"
    mock_collection.config.get.return_value.multi_tenancy_config.enabled = False
    mock_collection.with_consistency_level.return_value.data.delete_by_id.return_value = (
        False
    )
    fake_client.collections.get.return_value = mock_collection

    result = _invoke(
        cli_runner,
        fake_client,
        "weaviate_cli.commands.delete",
        [
            "delete",
            "data",
            "--collection",
            "TestCollection",
            "--uuid",
            "00000000-0000-0000-0000-000000000000",
        ],
    )

    assert result.exit_code == 1, result.output
    assert "00000000-0000-0000-0000-000000000000" in result.output
    assert "not found" in result.output


# ---------------------------------------------------------------------------
# get alias -- client.alias.get() returns None on 404 (CLI handles it
# explicitly, but pin the contract).
# ---------------------------------------------------------------------------


def test_cli_get_alias_not_found_reports_not_found(cli_runner, fake_client):
    fake_client.alias.get.return_value = None

    result = _invoke(
        cli_runner,
        fake_client,
        "weaviate_cli.commands.get",
        ["get", "alias", "--alias_name", "ghost"],
    )

    # The get command currently treats a missing alias as a soft miss (exit 0
    # with an explanatory line). The important guard is that we never silently
    # render a None alias as if it existed.
    assert result.exit_code == 0, result.output
    assert "Alias 'ghost' not found." in result.output

import json
import pytest
from unittest.mock import MagicMock, patch
from weaviate_cli.managers.role_manager import RoleManager


@pytest.fixture
def mock_client():
    client = MagicMock()
    return client


@pytest.fixture
def role_manager(mock_client):
    return RoleManager(mock_client)


# ---------------------------------------------------------------------------
# create_role
# ---------------------------------------------------------------------------


def test_create_role_success_text(role_manager, mock_client, capsys):
    mock_client.roles.create.return_value = None

    with patch("weaviate_cli.managers.role_manager.parse_permission") as mock_parse:
        mock_parse.return_value = MagicMock()
        role_manager.create_role(
            role_name="test-role",
            permissions=("read_collections:Movies",),
            json_output=False,
        )

    out = capsys.readouterr().out
    assert "test-role" in out
    assert "created successfully" in out


def test_create_role_success_json(role_manager, mock_client, capsys):
    mock_client.roles.create.return_value = None

    with patch("weaviate_cli.managers.role_manager.parse_permission") as mock_parse:
        mock_parse.return_value = MagicMock()
        role_manager.create_role(
            role_name="test-role",
            permissions=("read_collections:Movies",),
            json_output=True,
        )

    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["status"] == "success"
    assert "test-role" in data["message"]


def test_create_role_error(role_manager, mock_client):
    mock_client.roles.create.side_effect = Exception("connection failed")

    with patch("weaviate_cli.managers.role_manager.parse_permission") as mock_parse:
        mock_parse.return_value = MagicMock()
        with pytest.raises(Exception) as exc_info:
            role_manager.create_role(
                role_name="test-role",
                permissions=("read_collections:Movies",),
            )

    assert "Error creating role 'test-role'" in str(exc_info.value)


# ---------------------------------------------------------------------------
# delete_role
# ---------------------------------------------------------------------------


def test_delete_role_success_text(role_manager, mock_client, capsys):
    mock_client.roles.exists.side_effect = [True, False]

    role_manager.delete_role(role_name="test-role", json_output=False)

    out = capsys.readouterr().out
    assert "test-role" in out
    assert "deleted successfully" in out


def test_delete_role_success_json(role_manager, mock_client, capsys):
    mock_client.roles.exists.side_effect = [True, False]

    role_manager.delete_role(role_name="test-role", json_output=True)

    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["status"] == "success"
    assert "test-role" in data["message"]


def test_delete_role_not_found(role_manager, mock_client):
    mock_client.roles.exists.return_value = False

    with pytest.raises(Exception) as exc_info:
        role_manager.delete_role(role_name="nonexistent-role")

    assert "does not exist" in str(exc_info.value)


# ---------------------------------------------------------------------------
# add_permission
# ---------------------------------------------------------------------------


def test_add_permission_success_text(role_manager, mock_client, capsys):
    mock_client.roles.add_permissions.return_value = None

    with patch("weaviate_cli.managers.role_manager.parse_permission") as mock_parse:
        mock_parse.return_value = MagicMock()
        role_manager.add_permission(
            permission=("read_collections:Movies",),
            role_name="test-role",
            json_output=False,
        )

    out = capsys.readouterr().out
    assert "test-role" in out
    assert "added" in out


def test_add_permission_success_json(role_manager, mock_client, capsys):
    mock_client.roles.add_permissions.return_value = None

    with patch("weaviate_cli.managers.role_manager.parse_permission") as mock_parse:
        mock_parse.return_value = MagicMock()
        role_manager.add_permission(
            permission=("read_collections:Movies",),
            role_name="test-role",
            json_output=True,
        )

    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["status"] == "success"
    assert "test-role" in data["message"]


def test_add_permission_error(role_manager, mock_client):
    mock_client.roles.add_permissions.side_effect = Exception("failed")

    with patch("weaviate_cli.managers.role_manager.parse_permission") as mock_parse:
        mock_parse.return_value = MagicMock()
        with pytest.raises(Exception) as exc_info:
            role_manager.add_permission(
                permission=("read_collections:Movies",),
                role_name="test-role",
            )

    assert "Error adding permission" in str(exc_info.value)


# ---------------------------------------------------------------------------
# revoke_permission
# ---------------------------------------------------------------------------


def test_revoke_permission_success_text(role_manager, mock_client, capsys):
    mock_client.roles.remove_permissions.return_value = None

    with patch("weaviate_cli.managers.role_manager.parse_permission") as mock_parse:
        mock_parse.return_value = MagicMock()
        role_manager.revoke_permission(
            permission=("read_collections:Movies",),
            role_name="test-role",
            json_output=False,
        )

    out = capsys.readouterr().out
    assert "test-role" in out
    assert "revoked" in out


def test_revoke_permission_success_json(role_manager, mock_client, capsys):
    mock_client.roles.remove_permissions.return_value = None

    with patch("weaviate_cli.managers.role_manager.parse_permission") as mock_parse:
        mock_parse.return_value = MagicMock()
        role_manager.revoke_permission(
            permission=("read_collections:Movies",),
            role_name="test-role",
            json_output=True,
        )

    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["status"] == "success"
    assert "test-role" in data["message"]


def test_revoke_permission_error(role_manager, mock_client):
    mock_client.roles.remove_permissions.side_effect = Exception("failed")

    with patch("weaviate_cli.managers.role_manager.parse_permission") as mock_parse:
        mock_parse.return_value = MagicMock()
        with pytest.raises(Exception) as exc_info:
            role_manager.revoke_permission(
                permission=("read_collections:Movies",),
                role_name="test-role",
            )

    assert "Error revoking permission" in str(exc_info.value)

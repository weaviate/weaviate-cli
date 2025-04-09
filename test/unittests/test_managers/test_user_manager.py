import pytest
from unittest.mock import Mock, patch
from weaviate_cli.managers.user_manager import UserManager
from weaviate.exceptions import WeaviateConnectionError


@pytest.fixture
def mock_client():
    client = Mock()
    return client


@pytest.fixture
def user_manager(mock_client):
    return UserManager(mock_client)


def test_get_user_from_role_success(user_manager):
    # Arrange
    role_name = "test_role"
    expected_users = ["user1", "user2"]
    user_manager.client.roles.get_assigned_user_ids.return_value = expected_users

    # Act
    result = user_manager.get_user_from_role(role_name)

    # Assert
    assert result == expected_users
    user_manager.client.roles.get_assigned_user_ids.assert_called_once_with(
        role_name=role_name
    )


def test_get_user_from_role_error(user_manager):
    # Arrange
    role_name = "test_role"
    user_manager.client.roles.get_assigned_user_ids.side_effect = Exception(
        "Test error"
    )

    # Act & Assert
    with pytest.raises(Exception) as exc_info:
        user_manager.get_user_from_role(role_name)
    assert (
        str(exc_info.value) == f"Error getting users for role '{role_name}': Test error"
    )


def test_create_user_success(user_manager):
    # Arrange
    user_name = "test_user"
    expected_api_key = "test_api_key"
    user_manager.client.users.db.create.return_value = expected_api_key

    # Act
    result = user_manager.create_user(user_name)

    # Assert
    assert result == expected_api_key
    user_manager.client.users.db.create.assert_called_once_with(user_id=user_name)


def test_create_user_no_name(user_manager):
    # Act & Assert
    with pytest.raises(Exception) as exc_info:
        user_manager.create_user(None)
    assert str(exc_info.value) == "User name is required."


def test_create_user_error(user_manager):
    # Arrange
    user_name = "test_user"
    user_manager.client.users.db.create.side_effect = Exception("Test error")

    # Act & Assert
    with pytest.raises(Exception) as exc_info:
        user_manager.create_user(user_name)
    assert str(exc_info.value) == f"Error creating user '{user_name}': Test error"


def test_update_user_rotate_key_success(user_manager):
    # Arrange
    user_name = "test_user"
    expected_api_key = "new_api_key"
    user_manager.client.users.db.rotate_key.return_value = expected_api_key

    # Act
    result = user_manager.update_user(user_name=user_name, rotate_api_key=True)

    # Assert
    assert result == expected_api_key
    user_manager.client.users.db.rotate_key.assert_called_once_with(user_id=user_name)


def test_update_user_activate_success(user_manager):
    # Arrange
    user_name = "test_user"
    user_manager.client.users.db.activate.return_value = None

    # Act
    result = user_manager.update_user(user_name=user_name, activate=True)

    # Assert
    assert result is None
    user_manager.client.users.db.activate.assert_called_once_with(user_id=user_name)


def test_update_user_deactivate_success(user_manager):
    # Arrange
    user_name = "test_user"
    user_manager.client.users.db.deactivate.return_value = None

    # Act
    result = user_manager.update_user(user_name=user_name, deactivate=True)

    # Assert
    assert result is None
    user_manager.client.users.db.deactivate.assert_called_once_with(user_id=user_name)


def test_update_user_invalid_combination(user_manager):
    # Arrange
    user_name = "test_user"

    # Act & Assert
    with pytest.raises(Exception) as exc_info:
        user_manager.update_user(
            user_name=user_name, rotate_api_key=True, activate=True
        )
    assert (
        str(exc_info.value)
        == "Cannot rotate api key and activate or deactivate user at the same time."
    )


def test_update_user_activate_deactivate(user_manager):
    # Arrange
    user_name = "test_user"

    # Act & Assert
    with pytest.raises(Exception) as exc_info:
        user_manager.update_user(user_name=user_name, activate=True, deactivate=True)
    assert (
        str(exc_info.value) == "Cannot activate and deactivate user at the same time."
    )


def test_update_user_no_name(user_manager):
    # Act & Assert
    with pytest.raises(Exception) as exc_info:
        user_manager.update_user(None)
    assert str(exc_info.value) == "User name is required."


def test_update_user_error(user_manager):
    # Arrange
    user_name = "test_user"
    user_manager.client.users.db.rotate_key.side_effect = Exception("Test error")

    # Act & Assert
    with pytest.raises(Exception) as exc_info:
        user_manager.update_user(user_name=user_name, rotate_api_key=True)
    assert str(exc_info.value) == f"Error updating user '{user_name}': Test error"


def test_delete_user_success(user_manager):
    # Arrange
    user_name = "test_user"

    # Act
    user_manager.delete_user(user_name)

    # Assert
    user_manager.client.users.db.delete.assert_called_once_with(user_id=user_name)


def test_delete_user_no_name(user_manager):
    # Act & Assert
    with pytest.raises(Exception) as exc_info:
        user_manager.delete_user(None)
    assert str(exc_info.value) == "User name is required."


def test_delete_user_error(user_manager):
    # Arrange
    user_name = "test_user"
    user_manager.client.users.db.delete.side_effect = Exception("Test error")

    # Act & Assert
    with pytest.raises(Exception) as exc_info:
        user_manager.delete_user(user_name)
    assert str(exc_info.value) == f"Error deleting user '{user_name}': Test error"


def test_add_role_db_success(user_manager):
    # Arrange
    role_name = ("test_role",)
    user_name = "test_user"
    user_manager.client.get_meta.return_value = {"version": "1.30.0"}

    # Act
    user_manager.add_role(role_name, user_name, "db")

    # Assert
    user_manager.client.users.db.assign_roles.assert_called_once_with(
        user_id=user_name, role_names=list(role_name)
    )


def test_add_role_db_success_pre_130(user_manager):
    # Arrange
    role_name = ("test_role",)
    user_name = "test_user"
    user_manager.client.get_meta.return_value = {"version": "1.29.0"}

    # Act
    user_manager.add_role(role_name, user_name, "db")

    # Assert
    user_manager.client.users.assign_roles.assert_called_once_with(
        user_id=user_name, role_names=list(role_name)
    )


def test_add_role_oidc_success(user_manager):
    # Arrange
    role_name = ("test_role",)
    user_name = "test_user"
    user_manager.client.get_meta.return_value = {"version": "1.30.0"}

    # Act
    user_manager.add_role(role_name, user_name, "oidc")

    # Assert
    user_manager.client.users.oidc.assign_roles.assert_called_once_with(
        user_id=user_name, role_names=list(role_name)
    )


def test_add_role_error(user_manager):
    # Arrange
    role_name = ("test_role",)
    user_name = "test_user"
    user_manager.client.get_meta.return_value = {"version": "1.30.0"}
    user_manager.client.users.db.assign_roles.side_effect = Exception("Test error")

    # Act & Assert
    with pytest.raises(Exception) as exc_info:
        user_manager.add_role(role_name, user_name, "db")
    assert (
        str(exc_info.value)
        == f"Error assigning db role '{role_name}' to user '{user_name}': Test error"
    )


def test_revoke_role_db_success(user_manager):
    # Arrange
    role_name = ("test_role",)
    user_name = "test_user"
    user_manager.client.get_meta.return_value = {"version": "1.30.0"}

    # Act
    user_manager.revoke_role(role_name, user_name, "db")

    # Assert
    user_manager.client.users.db.revoke_roles.assert_called_once_with(
        user_id=user_name, role_names=list(role_name)
    )


def test_revoke_role_db_success_pre_130(user_manager):
    # Arrange
    role_name = ("test_role",)
    user_name = "test_user"
    user_manager.client.get_meta.return_value = {"version": "1.29.0"}

    # Act
    user_manager.revoke_role(role_name, user_name, "db")

    # Assert
    user_manager.client.users.revoke_roles.assert_called_once_with(
        user_id=user_name, role_names=list(role_name)
    )


def test_revoke_role_oidc_success(user_manager):
    # Arrange
    role_name = ("test_role",)
    user_name = "test_user"
    user_manager.client.get_meta.return_value = {"version": "1.30.0"}
    # Act
    user_manager.revoke_role(role_name, user_name, "oidc")

    # Assert
    user_manager.client.users.oidc.revoke_roles.assert_called_once_with(
        user_id=user_name, role_names=list(role_name)
    )


def test_revoke_role_error(user_manager):
    # Arrange
    role_name = ("test_role",)
    user_name = "test_user"
    user_manager.client.users.db.revoke_roles.side_effect = Exception("Test error")
    user_manager.client.get_meta.return_value = {"version": "1.30.0"}

    # Act & Assert
    with pytest.raises(Exception) as exc_info:
        user_manager.revoke_role(role_name, user_name, "db")
    assert (
        str(exc_info.value)
        == f"Error revoking db role '{role_name}' from user '{user_name}': Test error"
    )


def test_print_user(user_manager, capsys):
    # Arrange
    user = "test_user"

    # Act
    user_manager.print_user(user)

    # Assert
    captured = capsys.readouterr()
    assert captured.out == f"User: {user}\n"

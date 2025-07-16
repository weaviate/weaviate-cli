import pytest
from unittest.mock import MagicMock
from weaviate.aliases.alias import AliasReturn

from weaviate_cli.managers.alias_manager import AliasManager


@pytest.fixture
def alias_manager(mock_client: MagicMock) -> AliasManager:
    """
    Returns an AliasManager instance with a mocked WeaviateClient.
    """
    mock_client.alias = MagicMock()
    return AliasManager(mock_client)


def test_create_alias_success(
    alias_manager: AliasManager, mock_client: MagicMock
) -> None:
    """
    Test successful alias creation.
    """
    alias_name = "test_alias"
    collection_name = "TestCollection"

    alias_manager.create_alias(alias_name, collection_name)

    mock_client.alias.create.assert_called_once_with(
        alias_name=alias_name, target_collection=collection_name
    )


def test_create_alias_error(
    alias_manager: AliasManager, mock_client: MagicMock
) -> None:
    """
    Test alias creation with an exception.
    """
    alias_name = "test_alias"
    collection_name = "TestCollection"
    error_message = "Failed to create"
    mock_client.alias.create.side_effect = Exception(error_message)

    with pytest.raises(Exception) as exc_info:
        alias_manager.create_alias(alias_name, collection_name)

    assert f"Error creating alias '{alias_name}': {error_message}" in str(
        exc_info.value
    )


def test_update_alias_success(
    alias_manager: AliasManager, mock_client: MagicMock
) -> None:
    """
    Test successful alias update.
    """
    alias_name = "test_alias"
    collection_name = "NewTestCollection"

    alias_manager.update_alias(alias_name, collection_name)

    mock_client.alias.update.assert_called_once_with(
        alias_name=alias_name, new_target_collection=collection_name
    )


def test_update_alias_error(
    alias_manager: AliasManager, mock_client: MagicMock
) -> None:
    """
    Test alias update with an exception.
    """
    alias_name = "test_alias"
    collection_name = "NewTestCollection"
    error_message = "Failed to update"
    mock_client.alias.update.side_effect = Exception(error_message)

    with pytest.raises(Exception) as exc_info:
        alias_manager.update_alias(alias_name, collection_name)

    assert f"Error updating alias '{alias_name}': {error_message}" in str(
        exc_info.value
    )


def test_get_alias_success(alias_manager: AliasManager, mock_client: MagicMock) -> None:
    """
    Test successful alias retrieval.
    """
    alias_name = "test_alias"
    expected_alias = AliasReturn(alias=alias_name, collection="SomeCollection")
    mock_client.alias.get.return_value = expected_alias

    result = alias_manager.get_alias(alias_name)

    assert result == expected_alias
    mock_client.alias.get.assert_called_once_with(alias_name=alias_name)


def test_get_alias_error(alias_manager: AliasManager, mock_client: MagicMock) -> None:
    """
    Test alias retrieval with an exception.
    """
    alias_name = "test_alias"
    error_message = "Failed to get"
    mock_client.alias.get.side_effect = Exception(error_message)

    with pytest.raises(Exception) as exc_info:
        alias_manager.get_alias(alias_name)

    assert f"Error getting alias '{alias_name}': {error_message}" in str(exc_info.value)


def test_delete_alias_success(
    alias_manager: AliasManager, mock_client: MagicMock
) -> None:
    """
    Test successful alias deletion.
    """
    alias_name = "test_alias"

    alias_manager.delete_alias(alias_name)

    mock_client.alias.delete.assert_called_once_with(alias_name=alias_name)


def test_delete_alias_error(
    alias_manager: AliasManager, mock_client: MagicMock
) -> None:
    """
    Test alias deletion with an exception.
    """
    alias_name = "test_alias"
    error_message = "Failed to delete"
    mock_client.alias.delete.side_effect = Exception(error_message)

    with pytest.raises(Exception) as exc_info:
        alias_manager.delete_alias(alias_name)

    assert f"Error deleting alias '{alias_name}': {error_message}" in str(
        exc_info.value
    )


def test_list_aliases_all_success(
    alias_manager: AliasManager, mock_client: MagicMock
) -> None:
    """
    Test successful listing of all aliases.
    """
    expected_aliases = {
        "alias1": AliasReturn(alias="alias1", collection="collection1"),
        "alias2": AliasReturn(alias="alias2", collection="collection2"),
    }
    mock_client.alias.list_all.return_value = expected_aliases

    result = alias_manager.list_aliases()

    assert result == expected_aliases
    mock_client.alias.list_all.assert_called_once_with(collection=None)


def test_list_aliases_for_collection_success(
    alias_manager: AliasManager, mock_client: MagicMock
) -> None:
    """
    Test successful listing of aliases for a specific collection.
    """
    collection_name = "TestCollection"
    expected_aliases = {
        "alias1": AliasReturn(alias="alias1", collection="TestCollection"),
        "alias2": AliasReturn(alias="alias2", collection="TestCollection"),
    }
    mock_client.alias.list_all.return_value = expected_aliases

    result = alias_manager.list_aliases(collection=collection_name)

    assert result == expected_aliases
    mock_client.alias.list_all.assert_called_once_with(collection=collection_name)


def test_list_aliases_error(
    alias_manager: AliasManager, mock_client: MagicMock
) -> None:
    """
    Test listing aliases with an exception.
    """
    error_message = "Failed to list"
    mock_client.alias.list_all.side_effect = Exception(error_message)

    with pytest.raises(Exception) as exc_info:
        alias_manager.list_aliases()

    assert f"Error listing aliases: {error_message}" in str(exc_info.value)


def test_print_alias(alias_manager: AliasManager, capsys) -> None:
    """
    Test the output of the print_alias method.
    """
    alias_name = "my_alias"
    collection_name = "MyCollection"
    alias = AliasReturn(alias=alias_name, collection=collection_name)

    alias_manager.print_alias(alias)

    captured = capsys.readouterr()
    assert captured.out == f"Alias: {alias_name} -> {collection_name}\n"

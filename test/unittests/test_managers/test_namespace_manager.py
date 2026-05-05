import json
from types import SimpleNamespace

import pytest
from unittest.mock import MagicMock

from weaviate_cli.managers.namespace_manager import NamespaceManager


@pytest.fixture
def namespace_manager(mock_client: MagicMock) -> NamespaceManager:
    mock_client.namespaces = MagicMock()
    return NamespaceManager(mock_client)


def test_create_namespace_success_text(
    namespace_manager: NamespaceManager, mock_client: MagicMock, capsys
) -> None:
    name = "tenants_west"
    mock_client.namespaces.create.return_value = SimpleNamespace(name=name)

    result = namespace_manager.create_namespace(name=name)

    mock_client.namespaces.create.assert_called_once_with(name=name)
    assert result.name == name
    out = capsys.readouterr().out
    assert name in out
    assert "created successfully" in out


def test_create_namespace_success_json(
    namespace_manager: NamespaceManager, mock_client: MagicMock, capsys
) -> None:
    name = "tenants_west"
    mock_client.namespaces.create.return_value = SimpleNamespace(name=name)

    namespace_manager.create_namespace(name=name, json_output=True)

    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload["status"] == "success"
    assert payload["namespace"] == name


def test_create_namespace_requires_name(namespace_manager: NamespaceManager) -> None:
    with pytest.raises(Exception) as exc_info:
        namespace_manager.create_namespace(name="")
    assert "Namespace name is required." in str(exc_info.value)


def test_create_namespace_error(
    namespace_manager: NamespaceManager, mock_client: MagicMock
) -> None:
    mock_client.namespaces.create.side_effect = Exception("boom")

    with pytest.raises(Exception) as exc_info:
        namespace_manager.create_namespace(name="ns")
    assert "Error creating namespace 'ns': boom" in str(exc_info.value)


def test_get_namespace_returns_namespace(
    namespace_manager: NamespaceManager, mock_client: MagicMock
) -> None:
    expected = SimpleNamespace(name="ns")
    mock_client.namespaces.get.return_value = expected

    result = namespace_manager.get_namespace(name="ns")

    assert result is expected
    mock_client.namespaces.get.assert_called_once_with(name="ns")


def test_get_namespace_returns_none_when_missing(
    namespace_manager: NamespaceManager, mock_client: MagicMock
) -> None:
    mock_client.namespaces.get.return_value = None

    assert namespace_manager.get_namespace(name="ns") is None


def test_get_namespace_requires_name(namespace_manager: NamespaceManager) -> None:
    with pytest.raises(Exception) as exc_info:
        namespace_manager.get_namespace(name="")
    assert "Namespace name is required." in str(exc_info.value)


def test_list_namespaces_success(
    namespace_manager: NamespaceManager, mock_client: MagicMock
) -> None:
    expected = [SimpleNamespace(name="a"), SimpleNamespace(name="b")]
    mock_client.namespaces.list_all.return_value = expected

    result = namespace_manager.list_namespaces()

    assert result == expected
    mock_client.namespaces.list_all.assert_called_once_with()


def test_list_namespaces_error(
    namespace_manager: NamespaceManager, mock_client: MagicMock
) -> None:
    mock_client.namespaces.list_all.side_effect = Exception("bad")

    with pytest.raises(Exception) as exc_info:
        namespace_manager.list_namespaces()
    assert "Error listing namespaces: bad" in str(exc_info.value)


def test_delete_namespace_success_text(
    namespace_manager: NamespaceManager, mock_client: MagicMock, capsys
) -> None:
    namespace_manager.delete_namespace(name="ns")

    mock_client.namespaces.delete.assert_called_once_with(name="ns")
    out = capsys.readouterr().out
    assert "ns" in out
    assert "deleted successfully" in out


def test_delete_namespace_success_json(
    namespace_manager: NamespaceManager, mock_client: MagicMock, capsys
) -> None:
    namespace_manager.delete_namespace(name="ns", json_output=True)

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "success"
    assert "ns" in payload["message"]


def test_delete_namespace_requires_name(
    namespace_manager: NamespaceManager,
) -> None:
    with pytest.raises(Exception) as exc_info:
        namespace_manager.delete_namespace(name="")
    assert "Namespace name is required." in str(exc_info.value)


def test_delete_namespace_error(
    namespace_manager: NamespaceManager, mock_client: MagicMock
) -> None:
    mock_client.namespaces.delete.side_effect = Exception("nope")

    with pytest.raises(Exception) as exc_info:
        namespace_manager.delete_namespace(name="ns")
    assert "Error deleting namespace 'ns': nope" in str(exc_info.value)


def test_print_namespace_text(namespace_manager: NamespaceManager, capsys) -> None:
    namespace_manager.print_namespace(SimpleNamespace(name="ns"))
    assert capsys.readouterr().out.strip() == "Namespace: ns"


def test_print_namespace_json(namespace_manager: NamespaceManager, capsys) -> None:
    namespace_manager.print_namespace(SimpleNamespace(name="ns"), json_output=True)
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"name": "ns"}

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


def test_create_namespace_with_home_node(
    namespace_manager: NamespaceManager, mock_client: MagicMock, capsys
) -> None:
    name = "tenants_west"
    mock_client.namespaces.create.return_value = SimpleNamespace(
        name=name, home_node="node1", state="active"
    )

    namespace_manager.create_namespace(name=name, home_node="node1")

    mock_client.namespaces.create.assert_called_once_with(name=name, home_node="node1")
    assert "created successfully" in capsys.readouterr().out


def test_create_namespace_omits_home_node_when_absent(
    namespace_manager: NamespaceManager, mock_client: MagicMock
) -> None:
    name = "tenants_west"
    mock_client.namespaces.create.return_value = SimpleNamespace(name=name)

    namespace_manager.create_namespace(name=name)

    # home_node must not be forwarded when not provided (older-cluster compatible).
    mock_client.namespaces.create.assert_called_once_with(name=name)


def test_create_namespace_json_includes_home_node_and_state(
    namespace_manager: NamespaceManager, mock_client: MagicMock, capsys
) -> None:
    name = "tenants_west"
    mock_client.namespaces.create.return_value = SimpleNamespace(
        name=name, home_node="node1", state="active"
    )

    namespace_manager.create_namespace(name=name, home_node="node1", json_output=True)

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "success"
    assert payload["namespace"] == name
    assert payload["home_node"] == "node1"
    assert payload["state"] == "active"


def test_namespace_to_dict_full() -> None:
    ns = SimpleNamespace(name="ns", home_node="node1", state="active")
    assert NamespaceManager.namespace_to_dict(ns) == {
        "name": "ns",
        "home_node": "node1",
        "state": "active",
    }


def test_namespace_to_dict_omits_unset_fields() -> None:
    assert NamespaceManager.namespace_to_dict(SimpleNamespace(name="ns")) == {
        "name": "ns"
    }
    ns = SimpleNamespace(name="ns", home_node=None, state=None)
    assert NamespaceManager.namespace_to_dict(ns) == {"name": "ns"}


def test_print_namespace_text_with_home_node_and_state(
    namespace_manager: NamespaceManager, capsys
) -> None:
    namespace_manager.print_namespace(
        SimpleNamespace(name="ns", home_node="node1", state="active")
    )
    out = capsys.readouterr().out
    assert "Namespace: ns" in out
    assert "Home node: node1" in out
    assert "State: active" in out


def test_print_namespace_json_with_home_node_and_state(
    namespace_manager: NamespaceManager, capsys
) -> None:
    namespace_manager.print_namespace(
        SimpleNamespace(name="ns", home_node="node1", state="active"),
        json_output=True,
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"name": "ns", "home_node": "node1", "state": "active"}


def test_update_namespace_success_text(
    namespace_manager: NamespaceManager, mock_client: MagicMock, capsys
) -> None:
    mock_client.namespaces.update.return_value = SimpleNamespace(
        name="ns", home_node="node2", state="active"
    )

    result = namespace_manager.update_namespace(name="ns", home_node="node2")

    mock_client.namespaces.update.assert_called_once_with(name="ns", home_node="node2")
    assert result.name == "ns"
    out = capsys.readouterr().out
    assert "updated successfully" in out
    assert "node2" in out


def test_update_namespace_success_json(
    namespace_manager: NamespaceManager, mock_client: MagicMock, capsys
) -> None:
    mock_client.namespaces.update.return_value = SimpleNamespace(
        name="ns", home_node="node2", state="active"
    )

    namespace_manager.update_namespace(name="ns", home_node="node2", json_output=True)

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "success"
    assert payload["namespace"] == "ns"
    assert payload["home_node"] == "node2"
    assert payload["state"] == "active"


def test_update_namespace_requires_name(
    namespace_manager: NamespaceManager,
) -> None:
    with pytest.raises(Exception) as exc_info:
        namespace_manager.update_namespace(name="", home_node="node2")
    assert "Namespace name is required." in str(exc_info.value)


def test_update_namespace_requires_home_node(
    namespace_manager: NamespaceManager,
) -> None:
    with pytest.raises(Exception) as exc_info:
        namespace_manager.update_namespace(name="ns", home_node="")
    assert "Home node is required." in str(exc_info.value)


def test_update_namespace_error(
    namespace_manager: NamespaceManager, mock_client: MagicMock
) -> None:
    mock_client.namespaces.update.side_effect = Exception("nope")

    with pytest.raises(Exception) as exc_info:
        namespace_manager.update_namespace(name="ns", home_node="node2")
    assert "Error updating namespace 'ns': nope" in str(exc_info.value)

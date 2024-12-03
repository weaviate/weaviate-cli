import pytest
from unittest.mock import MagicMock
from weaviate_cli.utils import (
    get_client_from_context,
    get_random_string,
    pp_objects,
    parse_permission,
)
from weaviate.collections import Collection
from io import StringIO
import sys
from weaviate.rbac.models import RBAC


def test_get_client_from_context(mock_click_context, mock_client):
    mock_click_context.obj["config"].get_client.return_value = mock_client
    client = get_client_from_context(mock_click_context)
    assert client == mock_client
    mock_click_context.obj["config"].get_client.assert_called_once()


def test_get_random_string():
    # Test different lengths
    assert len(get_random_string(5)) == 5
    assert len(get_random_string(10)) == 10

    # Test randomness
    str1 = get_random_string(8)
    str2 = get_random_string(8)
    assert str1 != str2

    # Test only lowercase letters
    random_str = get_random_string(20)
    assert all(c.islower() for c in random_str)


def test_pp_objects_empty():
    # Test empty response
    response = MagicMock()
    response.objects = []

    # Capture stdout
    captured_output = StringIO()
    sys.stdout = captured_output

    pp_objects(response, ["name", "description"])

    sys.stdout = sys.__stdout__
    assert "No objects found" in captured_output.getvalue()


def test_pp_objects_with_data():
    # Mock response object
    response = MagicMock()
    mock_obj = MagicMock()
    mock_obj.uuid = "test-uuid-1234"
    mock_obj.properties = {"name": "test_name", "description": "test_description"}
    mock_obj.metadata.distance = 0.5
    mock_obj.metadata.certainty = 0.8
    mock_obj.metadata.score = 0.9
    response.objects = [mock_obj]

    # Capture stdout
    captured_output = StringIO()
    sys.stdout = captured_output

    pp_objects(response, ["name", "description"])

    sys.stdout = sys.__stdout__
    output = captured_output.getvalue()

    # Verify output contains expected data
    assert "test-uuid-1234" in output
    assert "test_name" in output
    assert "test_description" in output
    assert "0.5" in output
    assert "0.8" in output
    assert "0.9" in output
    assert "Total: 1 objects" in output


def test_pp_objects_missing_properties():
    # Test handling of missing properties
    response = MagicMock()
    mock_obj = MagicMock()
    mock_obj.uuid = "test-uuid-5678"
    mock_obj.properties = {"name": "test_name"}  # Missing description
    mock_obj.metadata.distance = None
    mock_obj.metadata.certainty = None
    mock_obj.metadata.score = None
    response.objects = [mock_obj]

    captured_output = StringIO()
    sys.stdout = captured_output

    pp_objects(response, ["name", "description"])

    sys.stdout = sys.__stdout__
    output = captured_output.getvalue()

    assert "test-uuid-5678" in output
    assert "test_name" in output
    assert "None" in output


def test_parse_permission_collections():
    # Test basic schema permissions
    assert parse_permission("read_collections:Movies") == [
        RBAC.permissions.collections.read(collection="Movies")
    ]

    assert parse_permission("create_collections:*") == [
        RBAC.permissions.collections.create(collection="*")
    ]

    # Test without collection specified (should default to "*")
    assert parse_permission("update_collections") == [
        RBAC.permissions.collections.update(collection="*")
    ]

    assert parse_permission("delete_collections:Movies") == [
        RBAC.permissions.collections.delete(collection="Movies")
    ]

    # Test schema with * wildcard
    assert parse_permission("create_collections:*data*") == [
        RBAC.permissions.collections.create(collection="*data*")
    ]

    assert parse_permission("update_collections:*") == [
        RBAC.permissions.collections.update(collection="*")
    ]

    assert parse_permission("manage_collections") == [
        RBAC.permissions.collections.manage(collection="*")
    ]

    # Test schema with crud shorthand
    assert parse_permission("crud_collections:MyCollection*") == [
        RBAC.permissions.collections.create(collection="MyCollection*"),
        RBAC.permissions.collections.read(collection="MyCollection*"),
        RBAC.permissions.collections.update(collection="MyCollection*"),
        RBAC.permissions.collections.delete(collection="MyCollection*"),
    ]


def test_parse_permission_roles():
    # Test roles permissions
    assert parse_permission("manage_roles") == [RBAC.permissions.roles.manage()]

    assert parse_permission("read_roles") == [RBAC.permissions.roles.read()]

    assert parse_permission("manage_roles:TenantReader") == [
        RBAC.permissions.roles.manage(role="TenantReader")
    ]

    assert parse_permission("read_roles:TenantReader") == [
        RBAC.permissions.roles.read(role="TenantReader")
    ]


def test_parse_permission_standalone():
    # Test standalone permissions
    assert parse_permission("manage_users") == [RBAC.permissions.users.manage()]
    assert parse_permission("read_cluster") == [RBAC.permissions.cluster.read()]


def test_parse_permission_backup():
    # Test manage_backups permission
    assert parse_permission("manage_backups") == [
        RBAC.permissions.backups.manage(collection="*")
    ]

    assert parse_permission("manage_backups:Movies") == [
        RBAC.permissions.backups.manage(collection="Movies")
    ]

    assert parse_permission("manage_backups:User.*") == [
        RBAC.permissions.backups.manage(collection="User.*")
    ]


def test_parse_permission_nodes():
    assert parse_permission("read_nodes:verbose") == [
        RBAC.permissions.nodes.read(verbosity="verbose")
    ]

    assert parse_permission("read_nodes:minimal") == [
        RBAC.permissions.nodes.read(verbosity="minimal")
    ]

    assert parse_permission("read_nodes:verbose:Movies") == [
        RBAC.permissions.nodes.read(verbosity="verbose", collection="Movies")
    ]


def test_parse_permission_crud():
    # Test crud shorthand for schema
    crud_perms = parse_permission("crud_collections")
    expected_crud = [
        RBAC.permissions.collections.create(collection="*"),
        RBAC.permissions.collections.read(collection="*"),
        RBAC.permissions.collections.update(collection="*"),
        RBAC.permissions.collections.delete(collection="*"),
    ]

    assert crud_perms == expected_crud

    # Test crud shorthand for specific collection
    crud_tenant_perms = parse_permission("crud_collections:Movies")
    expected_tenant_crud = [
        RBAC.permissions.collections.create(collection="Movies"),
        RBAC.permissions.collections.read(collection="Movies"),
        RBAC.permissions.collections.update(collection="Movies"),
        RBAC.permissions.collections.delete(collection="Movies"),
    ]
    assert crud_tenant_perms == expected_tenant_crud


def test_parse_permission_partial_crud():
    # Test cr (create, read) shorthand
    cr_perms = parse_permission("cr_collections:Movies")
    expected_cr = [
        RBAC.permissions.collections.create(collection="Movies"),
        RBAC.permissions.collections.read(collection="Movies"),
    ]
    assert cr_perms == expected_cr

    # Test ud (update, delete) shorthand
    ud_perms = parse_permission("ud_collections:Movies")
    expected_ud = [
        RBAC.permissions.collections.update(collection="Movies"),
        RBAC.permissions.collections.delete(collection="Movies"),
    ]
    assert ud_perms == expected_ud

    # Test ru (read, update) shorthand
    ru_perms = parse_permission("ru_collections:Movies")
    expected_ru = [
        RBAC.permissions.collections.read(collection="Movies"),
        RBAC.permissions.collections.update(collection="Movies"),
    ]
    assert ru_perms == expected_ru


def test_parse_permission_data():
    # Test crud shorthand for data
    crud_perms = parse_permission("crud_data")
    expected_crud = [
        RBAC.permissions.data.create(collection="*"),
        RBAC.permissions.data.read(collection="*"),
        RBAC.permissions.data.update(collection="*"),
        RBAC.permissions.data.delete(collection="*"),
    ]
    assert crud_perms == expected_crud

    # Test individual permissions
    create_perm = parse_permission("create_data:Movies")
    assert create_perm == [RBAC.permissions.data.create(collection="Movies")]

    read_perm = parse_permission("read_data:Movies")
    assert read_perm == [RBAC.permissions.data.read(collection="Movies")]

    update_perm = parse_permission("update_data:Movies")
    assert update_perm == [RBAC.permissions.data.update(collection="Movies")]

    delete_perm = parse_permission("delete_data:Movies")
    assert delete_perm == [RBAC.permissions.data.delete(collection="Movies")]

    manage_perm = parse_permission("manage_data:Movies")
    assert manage_perm == [RBAC.permissions.data.manage(collection="Movies")]


def test_parse_permission_invalid():
    # Test invalid action
    with pytest.raises(ValueError, match="Invalid resource type: action"):
        parse_permission("invalid_action:Movies")

    # Test invalid crud combination
    with pytest.raises(ValueError, match="Invalid crud combination: xyz"):
        parse_permission("xyz_collections:Movies")

    # Test invalid format
    with pytest.raises(ValueError, match="Invalid permission format"):
        parse_permission("read_collections:Movies:extra")

    with pytest.raises(ValueError, match="Invalid permission format"):
        parse_permission("read_nodes:verbose:Movies:extra")

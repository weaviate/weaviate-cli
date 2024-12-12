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
from weaviate.rbac.models import Permissions


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
    # Test basic collection permissions
    assert parse_permission("read_collections:Movies") == Permissions.collections(
        collection="Movies", read_config=True
    )

    # Test multiple collections
    assert parse_permission(
        "create_collections:Movies,Books,Films"
    ) == Permissions.collections(
        collection=["Movies", "Books", "Films"], create_collection=True
    )

    # Test crud collections
    assert parse_permission("crud_collections:Movies") == Permissions.collections(
        collection="Movies",
        create_collection=True,
        read_config=True,
        update_config=True,
        delete_collection=True,
    )


def test_parse_permission_data():
    # Test basic data permissions
    assert parse_permission("read_data:Movies") == Permissions.data(
        collection="Movies", read=True
    )

    # Test multiple collections
    assert parse_permission("create_data:Movies,Books,Films") == Permissions.data(
        collection=["Movies", "Books", "Films"], create=True
    )

    # Test crud data
    assert parse_permission("crud_data:Movies") == Permissions.data(
        collection="Movies", create=True, read=True, update=True, delete=True
    )


def test_parse_permission_roles():
    # Test basic role permissions
    assert parse_permission("read_roles:custom") == Permissions.roles(
        role="custom", read=True
    )

    # Test multiple roles
    assert parse_permission("read_roles:custom,editor,viewer") == Permissions.roles(
        role=["custom", "editor", "viewer"], read=True
    )

    # Test manage roles
    assert parse_permission("manage_roles:custom") == Permissions.roles(
        role="custom", manage=True
    )


def test_parse_permission_backups():
    # Test basic backup permissions
    assert parse_permission("manage_backups:Movies") == Permissions.backup(
        collection="Movies", manage=True
    )

    # Test multiple collections
    assert parse_permission("manage_backups:Movies,Books,Films") == Permissions.backup(
        collection=["Movies", "Books", "Films"], manage=True
    )


def test_parse_permission_nodes():
    # Test basic node permissions
    assert parse_permission("read_nodes:minimal:Movies") == Permissions.nodes(
        collection="Movies", verbosity="minimal", read=True
    )

    # Test verbose node permissions
    assert parse_permission("read_nodes:verbose:Movies") == Permissions.nodes(
        collection="Movies", verbosity="verbose", read=True
    )

    # Test multiple collections
    assert parse_permission(
        "read_nodes:verbose:Movies,Books,Films"
    ) == Permissions.nodes(
        collection=["Movies", "Books", "Films"], verbosity="verbose", read=True
    )


def test_parse_permission_cluster():
    # Test cluster read permission
    assert parse_permission("read_cluster") == Permissions.cluster(read=True)


def test_parse_permission_invalid():
    # Test invalid action
    with pytest.raises(ValueError, match="Invalid resource type: action"):
        parse_permission("invalid_action:Movies")

    # Test invalid crud combination
    with pytest.raises(ValueError, match="Invalid crud combination: xyz"):
        parse_permission("xyz_collections:Movies")

    # Test invalid node verbosity
    with pytest.raises(ValueError, match="Input should be 'minimal' or 'verbose'"):
        parse_permission("read_nodes:invalid:Movies")

    # Test missing node verbosity
    with pytest.raises(ValueError, match="Input should be 'minimal' or 'verbose'"):
        parse_permission("read_nodes:Movies")

    # Test passing too many parts
    with pytest.raises(ValueError, match="Invalid permission format"):
        parse_permission("read_nodes:minimal:Movies:custom")

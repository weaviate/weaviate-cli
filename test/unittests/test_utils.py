import pytest
from unittest.mock import MagicMock
from weaviate_cli.utils import get_client_from_context, get_random_string, pp_objects
from weaviate.collections import Collection
from io import StringIO
import sys


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

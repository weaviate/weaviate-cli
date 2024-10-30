import pytest
import weaviate
from unittest.mock import MagicMock, patch
from weaviate_cli.managers.config_manager import ConfigManager


@pytest.fixture
def mock_client():
    client = MagicMock(spec=weaviate.WeaviateClient)
    return client


@pytest.fixture
def mock_config():
    config = MagicMock(spec=ConfigManager)
    return config


@pytest.fixture
def mock_click_context(mock_config):
    ctx = MagicMock()
    ctx.obj = {"config": mock_config}
    return ctx

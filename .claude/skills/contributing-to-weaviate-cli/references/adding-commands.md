# Adding Commands Reference

Complete worked example for adding a new command to weaviate-cli.

## Step 1: Add Defaults Dataclass

In `weaviate_cli/defaults.py`, add a dataclass with default values:

```python
@dataclass
class CreateWidgetDefaults:
    collection: str = "Movies"
    widget_name: str = "default-widget"
    size: int = 100
    enabled: bool = False
```

## Step 2: Add Click Command

In the appropriate `weaviate_cli/commands/<group>.py` file, add the command:

```python
from weaviate_cli.defaults import CreateWidgetDefaults

@create.command("widget")
@click.option(
    "--collection",
    default=CreateWidgetDefaults.collection,
    help="The collection to create the widget in.",
    shell_complete=collection_name_complete,
)
@click.option(
    "--widget_name",
    default=CreateWidgetDefaults.widget_name,
    help="Name of the widget.",
)
@click.option(
    "--size",
    default=CreateWidgetDefaults.size,
    help=f"Widget size (default: {CreateWidgetDefaults.size}).",
    type=int,
)
@click.option(
    "--enabled",
    is_flag=True,
    help="Enable the widget (default: False).",
)
@click.option(
    "--json", "json_output", is_flag=True, default=False, help="Output in JSON format."
)
@click.pass_context
def create_widget_cli(ctx, collection, widget_name, size, enabled, json_output):
    """Create a widget in Weaviate."""
    client = None
    try:
        client = get_client_from_context(ctx)
        manager = WidgetManager(client)
        manager.create_widget(
            collection=collection,
            widget_name=widget_name,
            size=size,
            enabled=enabled,
            json_output=json_output,
        )
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()
```

### Key patterns to follow:

- **Parameter naming:** `--json` is aliased to `json_output` to avoid shadowing Python's `json` module
- **Default references:** Always use `CreateWidgetDefaults.field_name`, never hardcode defaults
- **Client lifecycle:** Get client in try, close in finally, echo error and exit(1) in except
- **Shell completion:** Use `shell_complete=collection_name_complete` for collection options
- **Boolean flags:** Use `is_flag=True` for boolean options
- **Choice options:** Use `type=click.Choice([...])` for constrained values
- **Help text:** Include default value in help string

## Step 3: Add Manager

Create `weaviate_cli/managers/widget_manager.py` (or add to an existing manager):

```python
import click
import json
from weaviate import WeaviateClient
from weaviate_cli.utils import print_json_or_text


class WidgetManager:
    def __init__(self, client: WeaviateClient):
        self.client = client

    def create_widget(
        self,
        collection: str,
        widget_name: str,
        size: int,
        enabled: bool,
        json_output: bool = False,
    ):
        # Validation
        if not self.client.collections.exists(collection):
            raise Exception(f"Collection '{collection}' does not exist.")

        # Business logic
        result = self.client.collections.get(collection).do_something(
            name=widget_name, size=size, enabled=enabled
        )

        # Output
        data = {
            "status": "success",
            "message": f"Widget '{widget_name}' created in collection '{collection}'.",
            "widget": {"name": widget_name, "size": size, "enabled": enabled},
        }
        print_json_or_text(
            data=data,
            json_output=json_output,
            text_fn=lambda: click.echo(
                f"Widget '{widget_name}' created successfully in '{collection}'."
            ),
        )
```

### Manager patterns:

- Raise exceptions with descriptive messages (caught by command's except block)
- Use `print_json_or_text()` for dual output support
- Accept `json_output: bool = False` parameter
- Don't call `sys.exit()` from managers -- let the command handle it

## Step 4: Add Unit Test

Create `test/unittests/test_managers/test_widget_manager.py`:

```python
import pytest
from unittest.mock import MagicMock
from weaviate_cli.managers.widget_manager import WidgetManager


def test_create_widget(mock_client):
    # Setup mock chain
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.return_value = True
    mock_collection = MagicMock()
    mock_collections.get.return_value = mock_collection

    manager = WidgetManager(mock_client)
    manager.create_widget(
        collection="TestCollection",
        widget_name="test-widget",
        size=50,
        enabled=True,
    )

    mock_collections.exists.assert_called_once_with("TestCollection")
    mock_collections.get.assert_called_once_with("TestCollection")


def test_create_widget_collection_not_found(mock_client):
    mock_client.collections.exists.return_value = False
    manager = WidgetManager(mock_client)

    with pytest.raises(Exception) as exc_info:
        manager.create_widget(
            collection="NonExistent",
            widget_name="test-widget",
            size=50,
        )

    assert "does not exist" in str(exc_info.value)
```

### Test patterns:

- Use `mock_client` fixture from `conftest.py`
- Test happy path: verify correct calls were made
- Test error path: verify exception raised with correct message
- Mock chains: `mock_client.collections.get.return_value = mock_collection`

## Step 5: Add Import

If you created a new manager, import it in the command file:

```python
from weaviate_cli.managers.widget_manager import WidgetManager
```

## Step 6: Update Documentation

1. Add the command to `.claude/skills/operating-weaviate-cli/SKILL.md` command reference
2. Update or create the relevant reference file in `.claude/skills/operating-weaviate-cli/references/`
3. Update `CLAUDE.md` if the command introduces new patterns

## Checklist

- [ ] Defaults dataclass in `defaults.py`
- [ ] Click command with `--json` support
- [ ] Manager with `print_json_or_text()` output
- [ ] Unit test (happy path + error path)
- [ ] Import in command file
- [ ] Black formatting passes (`make format && make lint`)
- [ ] Skill documentation updated

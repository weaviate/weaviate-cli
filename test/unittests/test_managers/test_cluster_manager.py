import json
import pytest
from unittest.mock import MagicMock, call

from weaviate_cli.managers.cluster_manager import ClusterManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_rep(
    uuid="test-uuid-1234",
    transfer_type_value="COPY",
    collection="Movies",
    shard="shard1",
    source_node="node0",
    target_node="node1",
    state_value="READY",
    errors=None,
    status_history=None,
):
    """Build a MagicMock replication object with the expected attribute shape."""
    rep = MagicMock()
    rep.uuid = uuid
    rep.transfer_type.value = transfer_type_value
    rep.collection = collection
    rep.shard = shard
    rep.source_node = source_node
    rep.target_node = target_node
    rep.status.state.value = state_value
    rep.status.errors = errors if errors is not None else []
    rep.status_history = status_history
    return rep


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def output_lines():
    """A list that accumulates lines emitted by the printer callable."""
    return []


@pytest.fixture
def cluster_manager(mock_client: MagicMock, output_lines: list) -> ClusterManager:
    """ClusterManager wired to a mocked client and a capturing printer."""
    mock_client.cluster = MagicMock()
    return ClusterManager(mock_client, printer=output_lines.append)


# ---------------------------------------------------------------------------
# start_replication
# ---------------------------------------------------------------------------


def test_start_replication_success(
    cluster_manager: ClusterManager, mock_client: MagicMock
) -> None:
    """start_replication returns a UUID string on success."""
    expected_uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    mock_client.cluster.replicate.return_value = expected_uuid

    result = cluster_manager.start_replication(
        collection="Movies",
        shard="shard0",
        source_node="node0",
        target_node="node1",
        type_="COPY",
    )

    assert result == expected_uuid
    mock_client.cluster.replicate.assert_called_once()


def test_start_replication_error(
    cluster_manager: ClusterManager, mock_client: MagicMock
) -> None:
    """start_replication wraps client errors with a descriptive message."""
    mock_client.cluster.replicate.side_effect = Exception("network failure")

    with pytest.raises(Exception) as exc_info:
        cluster_manager.start_replication(
            collection="Movies",
            shard="shard0",
            source_node="node0",
            target_node="node1",
            type_="COPY",
        )

    assert "Error starting replication" in str(exc_info.value)
    assert "network failure" in str(exc_info.value)


# ---------------------------------------------------------------------------
# delete_replication
# ---------------------------------------------------------------------------


def test_delete_replication_success(
    cluster_manager: ClusterManager, mock_client: MagicMock
) -> None:
    """delete_replication calls client.cluster.replications.delete with the uuid."""
    op_id = "test-op-id"

    cluster_manager.delete_replication(op_id)

    mock_client.cluster.replications.delete.assert_called_once_with(uuid=op_id)


def test_delete_replication_error(
    cluster_manager: ClusterManager, mock_client: MagicMock
) -> None:
    """delete_replication raises with the uuid and original message on error."""
    op_id = "bad-op-id"
    mock_client.cluster.replications.delete.side_effect = Exception("not found")

    with pytest.raises(Exception) as exc_info:
        cluster_manager.delete_replication(op_id)

    assert f"Error deleting replication with UUID '{op_id}'" in str(exc_info.value)
    assert "not found" in str(exc_info.value)


# ---------------------------------------------------------------------------
# delete_all_replication
# ---------------------------------------------------------------------------


def test_delete_all_replication_success(
    cluster_manager: ClusterManager, mock_client: MagicMock
) -> None:
    """delete_all_replication calls client.cluster.replications.delete_all."""
    cluster_manager.delete_all_replication()

    mock_client.cluster.replications.delete_all.assert_called_once_with()


def test_delete_all_replication_error(
    cluster_manager: ClusterManager, mock_client: MagicMock
) -> None:
    """delete_all_replication raises with a descriptive message on error."""
    mock_client.cluster.replications.delete_all.side_effect = Exception("server error")

    with pytest.raises(Exception) as exc_info:
        cluster_manager.delete_all_replication()

    assert "Error deleting all replications" in str(exc_info.value)
    assert "server error" in str(exc_info.value)


# ---------------------------------------------------------------------------
# cancel_replication
# ---------------------------------------------------------------------------


def test_cancel_replication_success(
    cluster_manager: ClusterManager, mock_client: MagicMock
) -> None:
    """cancel_replication calls client.cluster.replications.cancel with the uuid."""
    op_id = "cancel-op-id"

    cluster_manager.cancel_replication(op_id)

    mock_client.cluster.replications.cancel.assert_called_once_with(uuid=op_id)


def test_cancel_replication_error(
    cluster_manager: ClusterManager, mock_client: MagicMock
) -> None:
    """cancel_replication raises with the uuid and original message on error."""
    op_id = "bad-cancel-id"
    mock_client.cluster.replications.cancel.side_effect = Exception("timed out")

    with pytest.raises(Exception) as exc_info:
        cluster_manager.cancel_replication(op_id)

    assert f"Error cancelling replication with UUID '{op_id}'" in str(exc_info.value)
    assert "timed out" in str(exc_info.value)


# ---------------------------------------------------------------------------
# get_replication
# ---------------------------------------------------------------------------


def test_get_replication_success(
    cluster_manager: ClusterManager, mock_client: MagicMock
) -> None:
    """get_replication returns the object returned by the client."""
    op_id = "get-op-id"
    expected = _make_rep(uuid=op_id)
    mock_client.cluster.replications.get.return_value = expected

    result = cluster_manager.get_replication(op_id, include_history=False)

    assert result is expected
    mock_client.cluster.replications.get.assert_called_once_with(
        uuid=op_id, include_history=False
    )


def test_get_replication_with_history(
    cluster_manager: ClusterManager, mock_client: MagicMock
) -> None:
    """get_replication passes include_history=True to the client."""
    op_id = "get-op-id-history"
    expected = _make_rep(uuid=op_id)
    mock_client.cluster.replications.get.return_value = expected

    result = cluster_manager.get_replication(op_id, include_history=True)

    assert result is expected
    mock_client.cluster.replications.get.assert_called_once_with(
        uuid=op_id, include_history=True
    )


def test_get_replication_error(
    cluster_manager: ClusterManager, mock_client: MagicMock
) -> None:
    """get_replication raises with the uuid and original message on error."""
    op_id = "missing-id"
    mock_client.cluster.replications.get.side_effect = Exception("not found")

    with pytest.raises(Exception) as exc_info:
        cluster_manager.get_replication(op_id, include_history=False)

    assert f"Error getting replication with UUID '{op_id}'" in str(exc_info.value)
    assert "not found" in str(exc_info.value)


# ---------------------------------------------------------------------------
# get_all_replications
# ---------------------------------------------------------------------------


def test_get_all_replications_success(
    cluster_manager: ClusterManager, mock_client: MagicMock
) -> None:
    """get_all_replications returns the list returned by the client."""
    expected = [_make_rep(uuid="uuid-1"), _make_rep(uuid="uuid-2")]
    mock_client.cluster.replications.list_all.return_value = expected

    result = cluster_manager.get_all_replications()

    assert result is expected
    mock_client.cluster.replications.list_all.assert_called_once_with()


def test_get_all_replications_empty(
    cluster_manager: ClusterManager, mock_client: MagicMock
) -> None:
    """get_all_replications returns an empty list when there are no replications."""
    mock_client.cluster.replications.list_all.return_value = []

    result = cluster_manager.get_all_replications()

    assert result == []


def test_get_all_replications_error(
    cluster_manager: ClusterManager, mock_client: MagicMock
) -> None:
    """get_all_replications raises with a descriptive message on error."""
    mock_client.cluster.replications.list_all.side_effect = Exception("cluster down")

    with pytest.raises(Exception) as exc_info:
        cluster_manager.get_all_replications()

    assert "Error getting replications" in str(exc_info.value)
    assert "cluster down" in str(exc_info.value)


# ---------------------------------------------------------------------------
# print_replication — text output
# ---------------------------------------------------------------------------


def test_print_replication_text_basic(
    cluster_manager: ClusterManager, output_lines: list
) -> None:
    """print_replication emits all core fields via the printer."""
    rep = _make_rep()

    cluster_manager.print_replication(rep, json_output=False)

    combined = "\n".join(output_lines)
    assert "test-uuid-1234" in combined
    assert "COPY" in combined
    assert "Movies" in combined
    assert "shard1" in combined
    assert "node0" in combined
    assert "node1" in combined
    assert "READY" in combined


def test_print_replication_text_no_errors(
    cluster_manager: ClusterManager, output_lines: list
) -> None:
    """print_replication does not print an Errors section when errors is empty."""
    rep = _make_rep(errors=[])

    cluster_manager.print_replication(rep, json_output=False)

    combined = "\n".join(output_lines)
    assert "Errors:" not in combined
    assert "Last Error:" not in combined


def test_print_replication_text_with_errors(
    cluster_manager: ClusterManager, output_lines: list
) -> None:
    """print_replication prints the Errors section and last-error line when errors exist."""
    rep = _make_rep(errors=["disk full", "timeout"])

    cluster_manager.print_replication(rep, json_output=False)

    combined = "\n".join(output_lines)
    assert "Last Error:" in combined
    assert "disk full" in combined
    assert "Errors:" in combined
    assert "timeout" in combined


def test_print_replication_text_with_status_history(
    cluster_manager: ClusterManager, output_lines: list
) -> None:
    """print_replication prints Status History when status_history is not None."""
    history_entry = MagicMock()
    history_entry.state.value = "INDEXING"
    history_entry.errors = []

    rep = _make_rep(status_history=[history_entry])

    cluster_manager.print_replication(rep, json_output=False)

    combined = "\n".join(output_lines)
    assert "Status History:" in combined
    assert "INDEXING" in combined


def test_print_replication_text_no_status_history(
    cluster_manager: ClusterManager, output_lines: list
) -> None:
    """print_replication does not print Status History when status_history is None."""
    rep = _make_rep(status_history=None)

    cluster_manager.print_replication(rep, json_output=False)

    combined = "\n".join(output_lines)
    assert "Status History:" not in combined


# ---------------------------------------------------------------------------
# print_replication — json output
# ---------------------------------------------------------------------------


def test_print_replication_json_output(capsys) -> None:
    """print_replication with json_output=True emits valid JSON with expected fields."""
    mock_client = MagicMock()
    manager = ClusterManager(mock_client)
    rep = _make_rep()

    manager.print_replication(rep, json_output=True)

    captured = capsys.readouterr()
    data = json.loads(captured.out)

    assert data["uuid"] == "test-uuid-1234"
    assert data["type"] == "COPY"
    assert data["collection"] == "Movies"
    assert data["shard"] == "shard1"
    assert data["source_node"] == "node0"
    assert data["target_node"] == "node1"
    assert data["status"] == "READY"
    assert data["errors"] == []
    assert "status_history" not in data


def test_print_replication_json_output_with_status_history(capsys) -> None:
    """print_replication JSON includes status_history when it is not None."""
    mock_client = MagicMock()
    manager = ClusterManager(mock_client)

    history_entry = MagicMock()
    history_entry.state.value = "INDEXING"
    history_entry.errors = []

    rep = _make_rep(status_history=[history_entry])

    manager.print_replication(rep, json_output=True)

    captured = capsys.readouterr()
    data = json.loads(captured.out)

    assert "status_history" in data
    assert len(data["status_history"]) == 1
    assert data["status_history"][0]["state"] == "INDEXING"
    assert data["status_history"][0]["errors"] == []


def test_print_replication_json_output_with_errors(capsys) -> None:
    """print_replication JSON includes errors list when errors are present."""
    mock_client = MagicMock()
    manager = ClusterManager(mock_client)
    rep = _make_rep(errors=["write failed"])

    manager.print_replication(rep, json_output=True)

    captured = capsys.readouterr()
    data = json.loads(captured.out)

    assert data["errors"] == ["write failed"]


# ---------------------------------------------------------------------------
# print_replications — text output
# ---------------------------------------------------------------------------


def test_print_replications_text_non_empty(
    cluster_manager: ClusterManager, output_lines: list
) -> None:
    """print_replications renders a table containing all replication data."""
    rep1 = _make_rep(uuid="uuid-1111", collection="Movies", shard="shard1")
    rep2 = _make_rep(
        uuid="uuid-2222",
        collection="Books",
        shard="shard2",
        state_value="INDEXING",
        transfer_type_value="MOVE",
        source_node="node2",
        target_node="node3",
    )

    cluster_manager.print_replications([rep1, rep2], json_output=False)

    combined = "\n".join(output_lines)
    assert "uuid-1111" in combined
    assert "uuid-2222" in combined
    assert "Movies" in combined
    assert "Books" in combined
    assert "READY" in combined
    assert "INDEXING" in combined


def test_print_replications_text_empty(
    cluster_manager: ClusterManager, output_lines: list
) -> None:
    """print_replications prints a 'no replications' message for an empty list."""
    cluster_manager.print_replications([], json_output=False)

    combined = "\n".join(output_lines)
    assert "No replications found." in combined


# ---------------------------------------------------------------------------
# print_replications — json output
# ---------------------------------------------------------------------------


def test_print_replications_json_non_empty(capsys) -> None:
    """print_replications emits valid JSON with a 'replications' array."""
    mock_client = MagicMock()
    manager = ClusterManager(mock_client)

    rep1 = _make_rep(uuid="uuid-1111", collection="Movies", shard="shard1")
    rep2 = _make_rep(
        uuid="uuid-2222",
        collection="Books",
        shard="shard2",
        state_value="INDEXING",
        transfer_type_value="MOVE",
        source_node="node2",
        target_node="node3",
    )

    manager.print_replications([rep1, rep2], json_output=True)

    captured = capsys.readouterr()
    data = json.loads(captured.out)

    assert "replications" in data
    assert len(data["replications"]) == 2

    uuids = {r["uuid"] for r in data["replications"]}
    assert "uuid-1111" in uuids
    assert "uuid-2222" in uuids

    movies_rep = next(r for r in data["replications"] if r["collection"] == "Movies")
    assert movies_rep["status"] == "READY"
    assert movies_rep["shard"] == "shard1"
    assert movies_rep["source_node"] == "node0"
    assert movies_rep["target_node"] == "node1"
    assert movies_rep["type"] == "COPY"

    books_rep = next(r for r in data["replications"] if r["collection"] == "Books")
    assert books_rep["status"] == "INDEXING"
    assert books_rep["type"] == "MOVE"


def test_print_replications_json_empty(capsys) -> None:
    """print_replications emits {'replications': []} JSON for an empty list."""
    mock_client = MagicMock()
    manager = ClusterManager(mock_client)

    manager.print_replications([], json_output=True)

    captured = capsys.readouterr()
    data = json.loads(captured.out)

    assert data == {"replications": []}

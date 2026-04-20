"""Unit tests for `BasemapStateStore` (SQLite cold-storage for scraper resume)."""

import sqlite3

import pytest
import pytest_asyncio

from clients.basemap_state_store import BasemapStateStore, Cursor, ProviderHealth


@pytest_asyncio.fixture
async def store(tmp_path):
    """Fresh store backed by a tmp_path sqlite file; closed after each test."""
    db_path = tmp_path / "state.sqlite"
    s = BasemapStateStore(str(db_path))
    await s.connect()
    try:
        yield s
    finally:
        await s.close()


@pytest.mark.asyncio
async def test_connect_creates_parent_directory(tmp_path):
    """Missing parent dirs should be created automatically."""
    db_path = tmp_path / "nested" / "dir" / "state.sqlite"
    s = BasemapStateStore(str(db_path))
    await s.connect()
    try:
        assert db_path.exists()
    finally:
        await s.close()


@pytest.mark.asyncio
async def test_connect_is_idempotent(store):
    """Calling connect twice should be a no-op (connection stays open)."""
    await store.connect()
    # Still usable after second connect.
    await store.set_cursor("p", 3, 10)
    assert await store.get_cursor("p") == Cursor(zoom=3, tile_index=10)


@pytest.mark.asyncio
async def test_schema_idempotent(tmp_path):
    """Re-opening an existing DB should not fail (CREATE TABLE IF NOT EXISTS)."""
    db_path = tmp_path / "state.sqlite"
    s1 = BasemapStateStore(str(db_path))
    await s1.connect()
    await s1.set_cursor("p", 4, 17)
    await s1.close()

    s2 = BasemapStateStore(str(db_path))
    await s2.connect()
    try:
        assert await s2.get_cursor("p") == Cursor(zoom=4, tile_index=17)
    finally:
        await s2.close()


@pytest.mark.asyncio
async def test_get_cursor_missing_returns_none(store):
    assert await store.get_cursor("nope") is None


@pytest.mark.asyncio
async def test_set_cursor_inserts_then_upserts(store):
    await store.set_cursor("argenmap", 5, 100)
    assert await store.get_cursor("argenmap") == Cursor(zoom=5, tile_index=100)

    # Second call updates in place (no duplicate row; PK conflict).
    await store.set_cursor("argenmap", 6, 250)
    assert await store.get_cursor("argenmap") == Cursor(zoom=6, tile_index=250)


@pytest.mark.asyncio
async def test_clear_cursor_removes_row(store):
    await store.set_cursor("argenmap", 3, 1)
    await store.clear_cursor("argenmap")
    assert await store.get_cursor("argenmap") is None


@pytest.mark.asyncio
async def test_clear_cursor_missing_is_noop(store):
    """Clearing a non-existent cursor must not raise."""
    await store.clear_cursor("ghost")


@pytest.mark.asyncio
async def test_add_failed_is_idempotent(store):
    await store.add_failed("p", 5, 10, 20)
    await store.add_failed("p", 5, 10, 20)  # duplicate
    failed = await store.list_failed("p", 5)
    assert failed == [(10, 20)]


@pytest.mark.asyncio
async def test_list_failed_scoped_by_provider_and_zoom(store):
    await store.add_failed("p", 5, 1, 2)
    await store.add_failed("p", 5, 3, 4)
    await store.add_failed("p", 6, 9, 9)
    await store.add_failed("other", 5, 0, 0)

    at_p5 = sorted(await store.list_failed("p", 5))
    assert at_p5 == [(1, 2), (3, 4)]
    assert await store.list_failed("p", 6) == [(9, 9)]
    assert await store.list_failed("other", 5) == [(0, 0)]


@pytest.mark.asyncio
async def test_remove_failed_drops_single_entry(store):
    await store.add_failed("p", 5, 1, 2)
    await store.add_failed("p", 5, 3, 4)
    await store.remove_failed("p", 5, 1, 2)
    assert await store.list_failed("p", 5) == [(3, 4)]


@pytest.mark.asyncio
async def test_clear_failed_scopes_to_provider_and_zoom(store):
    await store.add_failed("p", 5, 1, 2)
    await store.add_failed("p", 6, 3, 4)
    await store.add_failed("other", 5, 9, 9)

    await store.clear_failed("p", 5)

    assert await store.list_failed("p", 5) == []
    assert await store.list_failed("p", 6) == [(3, 4)]
    assert await store.list_failed("other", 5) == [(9, 9)]


@pytest.mark.asyncio
async def test_clear_failed_for_provider_drops_all_zooms(store):
    await store.add_failed("p", 5, 1, 2)
    await store.add_failed("p", 6, 3, 4)
    await store.add_failed("p", 7, 5, 6)
    await store.add_failed("other", 5, 9, 9)

    await store.clear_failed_for_provider("p")

    assert await store.list_failed("p", 5) == []
    assert await store.list_failed("p", 6) == []
    assert await store.list_failed("p", 7) == []
    assert await store.list_failed("other", 5) == [(9, 9)]


@pytest.mark.asyncio
async def test_last_completed_missing_returns_none(store):
    assert await store.get_last_completed("nope") is None


@pytest.mark.asyncio
async def test_set_last_completed_inserts_then_upserts(store):
    await store.set_last_completed("argenmap", 1000)
    assert await store.get_last_completed("argenmap") == 1000

    await store.set_last_completed("argenmap", 2500)
    assert await store.get_last_completed("argenmap") == 2500


@pytest.mark.asyncio
async def test_last_completed_survives_reopen(tmp_path):
    """The completion stamp persists across store close/open (schema idempotent)."""
    db_path = tmp_path / "state.sqlite"
    s1 = BasemapStateStore(str(db_path))
    await s1.connect()
    await s1.set_last_completed("argenmap", 1700000000)
    await s1.close()

    s2 = BasemapStateStore(str(db_path))
    await s2.connect()
    try:
        assert await s2.get_last_completed("argenmap") == 1700000000
    finally:
        await s2.close()


@pytest.mark.asyncio
async def test_last_completed_scoped_per_provider(store):
    await store.set_last_completed("p1", 100)
    await store.set_last_completed("p2", 200)
    assert await store.get_last_completed("p1") == 100
    assert await store.get_last_completed("p2") == 200


@pytest.mark.asyncio
async def test_operations_without_connect_raise(tmp_path):
    """All public methods must require an active connection."""
    s = BasemapStateStore(str(tmp_path / "state.sqlite"))
    with pytest.raises(RuntimeError):
        await s.get_cursor("p")


@pytest.mark.asyncio
async def test_wal_mode_enabled(tmp_path):
    """Connection is opened in WAL journal mode for fast-but-durable writes."""
    db_path = tmp_path / "state.sqlite"
    s = BasemapStateStore(str(db_path))
    await s.connect()
    try:
        # Use a separate read-only connection to avoid races with the async lock.
        con = sqlite3.connect(str(db_path))
        mode = con.execute("PRAGMA journal_mode;").fetchone()[0]
        con.close()
        assert mode.lower() == "wal"
    finally:
        await s.close()


# --------------------------------------------------------------------------- #
# Provider health table
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_get_health_missing_returns_none(store):
    """No row yet → closed circuit (None)."""
    assert await store.get_health("novel") is None


@pytest.mark.asyncio
async def test_open_circuit_upserts_row(store):
    """open_circuit persists the circuit-breaker state."""
    await store.open_circuit(
        "p", consecutive_trips=2, cooldown_until=123456, reason="boom"
    )
    health = await store.get_health("p")
    assert isinstance(health, ProviderHealth)
    assert health.consecutive_trips == 2
    assert health.cooldown_until == 123456
    assert health.last_reason == "boom"
    assert health.last_tripped_at > 0


@pytest.mark.asyncio
async def test_open_circuit_is_idempotent_and_updates(store):
    """A second open_circuit call replaces the prior row rather than duplicating."""
    await store.open_circuit("p", 1, 100, "first")
    await store.open_circuit("p", 3, 900, "second")
    health = await store.get_health("p")
    assert health is not None
    assert health.consecutive_trips == 3
    assert health.cooldown_until == 900
    assert health.last_reason == "second"


@pytest.mark.asyncio
async def test_close_circuit_deletes_row(store):
    """close_circuit removes the health row entirely."""
    await store.open_circuit("p", 1, 100, "reason")
    await store.close_circuit("p")
    assert await store.get_health("p") is None


@pytest.mark.asyncio
async def test_close_circuit_on_missing_is_a_noop(store):
    """Closing a never-opened circuit is safe."""
    await store.close_circuit("never-tripped")
    assert await store.get_health("never-tripped") is None

"""Unit tests for `StorageCircuit`, the exponential-backoff storage gate."""

import pytest

from services.storage_circuit import StorageCircuit


class FakeClock:
    """Hand-driven monotonic clock so backoff is tested without sleeping."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _make_circuit(clock: FakeClock, **overrides) -> StorageCircuit:
    base = {
        "window": 10,
        "min_samples": 4,
        "threshold": 0.8,
        "base_cooldown": 1.0,
        "max_cooldown": 30.0,
        "max_consecutive_trips": 6,
        "time_fn": clock,
    }
    base.update(overrides)
    return StorageCircuit("S3", **base)


def _fail(circuit: StorageCircuit, times: int) -> list:
    return [circuit.record_failure() for _ in range(times)]


def test_starts_closed_and_allows_writes():
    circuit = _make_circuit(FakeClock())
    assert circuit.allows_write() is True
    assert circuit.is_open() is False
    assert circuit.cooldown_remaining() == 0.0


def test_stays_closed_below_min_samples():
    """A couple of failures must not open the circuit — blips ride through."""
    circuit = _make_circuit(FakeClock())
    transitions = _fail(circuit, 3)
    assert all(t is None for t in transitions)
    assert circuit.allows_write() is True


def test_opens_once_failure_rate_exceeds_threshold():
    clock = FakeClock()
    circuit = _make_circuit(clock)
    transitions = _fail(circuit, 4)
    opened = [t for t in transitions if t and t.opened]
    assert len(opened) == 1
    assert opened[0].cooldown == 1.0
    assert circuit.is_open() is True
    # Writes are refused while the cooldown runs.
    assert circuit.allows_write() is False


def test_scattered_failures_do_not_open():
    """Alternating success/failure stays under the threshold."""
    circuit = _make_circuit(FakeClock())
    for _ in range(10):
        circuit.record_failure()
        circuit.record_success()
    assert circuit.is_open() is False


def test_probe_window_opens_only_once_cooldown_elapses():
    clock = FakeClock()
    circuit = _make_circuit(clock)
    _fail(circuit, 4)

    assert circuit.allows_write() is False
    clock.advance(0.5)
    assert circuit.allows_write() is False
    clock.advance(0.5)
    assert circuit.allows_write() is True

    # A reported failure re-arms, closing the window again.
    circuit.record_failure()
    assert circuit.allows_write() is False


def test_concurrent_batch_advances_backoff_once_per_window():
    """A dispatched chunk must not burn the whole schedule in one go.

    Writes admitted before the trip (or sharing one probe window) report their
    failures late; only the first counts, or a single 500-tile fan-out would
    exhaust the circuit instantly and skip the pausing entirely.
    """
    clock = FakeClock()
    circuit = _make_circuit(clock, max_consecutive_trips=6)
    _fail(circuit, 4)  # opens; cooldown 1.0

    # 500 stragglers report after the trip, still inside the cooldown.
    for _ in range(500):
        assert circuit.record_failure() is None

    assert circuit.exhausted() is False
    assert circuit.cooldown_remaining() == pytest.approx(1.0)


def test_cooldown_doubles_on_each_failed_probe_and_caps():
    clock = FakeClock()
    circuit = _make_circuit(clock)
    _fail(circuit, 4)

    seen = []
    for _ in range(8):
        clock.advance(circuit.cooldown_remaining())
        assert circuit.allows_write() is True  # probe admitted
        transition = circuit.record_failure()
        assert transition is not None and transition.probe_failed
        seen.append(transition.cooldown)

    assert seen[:5] == [2.0, 4.0, 8.0, 16.0, 30.0]
    assert all(c == 30.0 for c in seen[5:]), "cooldown must cap at max_cooldown"


def test_probe_success_closes_circuit_and_reports_recovery():
    clock = FakeClock()
    circuit = _make_circuit(clock)
    _fail(circuit, 4)

    clock.advance(1.0)
    assert circuit.allows_write() is True
    clock.advance(4.0)
    transition = circuit.record_success()

    assert transition is not None
    assert transition.recovered is True
    assert transition.failures == 4
    assert transition.downtime == pytest.approx(5.0)
    assert circuit.is_open() is False
    assert circuit.allows_write() is True


def test_reopening_restarts_backoff_at_base():
    """Recovery resets the schedule, so a later outage isn't punished for it."""
    clock = FakeClock()
    circuit = _make_circuit(clock)
    _fail(circuit, 4)
    clock.advance(1.0)
    circuit.allows_write()
    circuit.record_success()

    transitions = _fail(circuit, 4)
    opened = [t for t in transitions if t and t.opened]
    assert len(opened) == 1
    assert opened[0].cooldown == 1.0


def test_exhausted_after_max_consecutive_probe_failures():
    clock = FakeClock()
    circuit = _make_circuit(clock, max_consecutive_trips=3)
    _fail(circuit, 4)
    assert circuit.exhausted() is False

    for _ in range(3):
        clock.advance(circuit.cooldown_remaining())
        circuit.allows_write()
        circuit.record_failure()

    assert circuit.exhausted() is True


def test_exhausted_resets_after_recovery():
    clock = FakeClock()
    circuit = _make_circuit(clock, max_consecutive_trips=2)
    _fail(circuit, 4)
    for _ in range(2):
        clock.advance(circuit.cooldown_remaining())
        circuit.allows_write()
        circuit.record_failure()
    assert circuit.exhausted() is True

    clock.advance(circuit.cooldown_remaining())
    circuit.allows_write()
    circuit.record_success()
    assert circuit.exhausted() is False


def test_closed_circuit_success_returns_no_transition():
    """Only edges log — the steady state must stay silent."""
    circuit = _make_circuit(FakeClock())
    assert all(circuit.record_success() is None for _ in range(50))

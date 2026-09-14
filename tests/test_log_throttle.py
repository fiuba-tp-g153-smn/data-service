"""Unit tests for `BackoffLogThrottle`."""

from log_throttle import BackoffLogThrottle


class FakeClock:
    def __init__(self) -> None:
        self.now = 500.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _make(clock: FakeClock, **overrides) -> BackoffLogThrottle:
    base = {
        "base_interval": 1.0,
        "max_interval": 30.0,
        "factor": 2.0,
        "time_fn": clock,
    }
    base.update(overrides)
    return BackoffLogThrottle(**base)


def test_first_occurrence_always_logs():
    throttle = _make(FakeClock())
    assert throttle.check("EndpointConnectionError") == 0


def test_suppresses_within_the_interval_and_counts():
    clock = FakeClock()
    throttle = _make(clock)
    throttle.check("k")

    assert all(throttle.check("k") is None for _ in range(99))

    clock.advance(1.0)
    assert throttle.check("k") == 99, "suppressed count must reach the log line"


def test_interval_doubles_and_caps():
    clock = FakeClock()
    throttle = _make(clock)
    throttle.check("k")

    gaps = []
    for _ in range(8):
        gap = 0.0
        # Walk forward until the throttle lets another line through.
        while throttle.check("k") is None:
            clock.advance(0.5)
            gap += 0.5
        gaps.append(gap)

    assert gaps[:5] == [1.0, 2.0, 4.0, 8.0, 16.0]
    assert all(g == 30.0 for g in gaps[5:]), "interval must cap at max_interval"


def test_keys_back_off_independently():
    clock = FakeClock()
    throttle = _make(clock)
    assert throttle.check("a") == 0
    assert throttle.check("b") == 0
    assert throttle.check("a") is None
    assert throttle.check("b") is None


def test_reset_rearms_the_key():
    """Recovery must not leave a key muted for a full interval."""
    clock = FakeClock()
    throttle = _make(clock)
    throttle.check("k")
    assert throttle.check("k") is None

    throttle.reset("k")
    assert throttle.check("k") == 0


def test_bounded_line_count_under_a_sustained_outage():
    """The property that matters: a flood costs a readable number of lines."""
    clock = FakeClock()
    throttle = _make(clock)

    logged = 0
    # 10 minutes of failures arriving every 10ms.
    for _ in range(60_000):
        if throttle.check("EndpointConnectionError") is not None:
            logged += 1
        clock.advance(0.01)

    assert logged <= 25, f"{logged} lines for a 10-minute outage is too many"

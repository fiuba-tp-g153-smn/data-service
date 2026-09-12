"""Exponential-backoff circuit breaker around a shared storage dependency."""

import time
from collections import deque
from dataclasses import dataclass
from typing import Callable, Deque, Optional

# Rolling-window judgement: don't open before `MIN_SAMPLES` outcomes, then open
# once the recent failure rate exceeds `THRESHOLD`. Mirrors the provider-health
# breaker in `basemap_scraper_service` so both read the same way.
DEFAULT_WINDOW = 20
DEFAULT_MIN_SAMPLES = 10
DEFAULT_THRESHOLD = 0.8
# Cooldown doubles from BASE up to MAX (1, 2, 4, 8, 16, 30, 30 ...). After
# MAX_CONSECUTIVE_TRIPS failed probes (~61s of pausing) the caller is told to
# stop waiting and hand back to its own scheduler.
DEFAULT_BASE_COOLDOWN = 1.0
DEFAULT_MAX_COOLDOWN = 30.0
DEFAULT_MAX_CONSECUTIVE_TRIPS = 6


@dataclass(frozen=True, slots=True)
class CircuitTransition:
    """A state change worth one log line. Returned only on the edge."""

    name: str
    opened: bool = False
    probe_failed: bool = False
    recovered: bool = False
    cooldown: float = 0.0
    failures: int = 0
    downtime: float = 0.0


class StorageCircuit:
    # pylint: disable=too-many-instance-attributes
    """Gate around a storage backend that fails wholesale rather than per-item.

    Storage is shared across every provider, so one instance guards the whole
    scraper: when S3 goes down each tile re-discovering that fact costs an
    upstream fetch, a SQLite row and a log line for nothing.

    Closed, writes flow. Open, `allows_write()` is False and the caller skips
    the work entirely until the cooldown expires, which opens a half-open probe
    window. A success in that window closes the circuit; a failure doubles the
    cooldown up to `max_cooldown`. Outcomes are reported via `record_success` /
    `record_failure`, which return a `CircuitTransition` only on a state edge —
    so a caller that logs every returned transition logs a handful of lines for
    an outage of any length, without a throttle.

    Admission is judged on the clock rather than by holding a probe slot: a
    caller that takes the slot and then never reports (its tile 404s, or the
    upstream is down) would otherwise wedge the circuit shut forever. The
    trade-off is that a batch dispatched concurrently can put several writes
    through one probe window — the first reported failure re-arms the cooldown
    and the rest are ignored as stragglers, so the backoff schedule still
    advances once per window rather than once per write.

    Not thread-safe by design: one asyncio event loop, and no awaits inside.
    """

    def __init__(
        self,
        name: str,
        *,
        window: int = DEFAULT_WINDOW,
        min_samples: int = DEFAULT_MIN_SAMPLES,
        threshold: float = DEFAULT_THRESHOLD,
        base_cooldown: float = DEFAULT_BASE_COOLDOWN,
        max_cooldown: float = DEFAULT_MAX_COOLDOWN,
        max_consecutive_trips: int = DEFAULT_MAX_CONSECUTIVE_TRIPS,
        time_fn: Callable[[], float] = time.monotonic,
    ):
        # pylint: disable=too-many-arguments
        self._name = name
        self._window = window
        self._min_samples = min_samples
        self._threshold = threshold
        self._base_cooldown = base_cooldown
        self._max_cooldown = max_cooldown
        self._max_consecutive_trips = max_consecutive_trips
        self._now = time_fn

        self._recent: Deque[bool] = deque()  # True = failure
        self._open_until: float = 0.0  # 0.0 => closed
        self._cooldown: float = base_cooldown
        self._consecutive_trips: int = 0
        self._failures_while_open: int = 0
        self._opened_at: float = 0.0

    @property
    def name(self) -> str:
        """Label used in the log line for this backend (e.g. "S3")."""
        return self._name

    def is_open(self) -> bool:
        """True while the circuit is tripped, cooldown expired or not."""
        return self._open_until > 0.0

    def exhausted(self) -> bool:
        """True once probes have failed enough that waiting is no longer worth it."""
        return self._consecutive_trips >= self._max_consecutive_trips

    def cooldown_remaining(self) -> float:
        """Seconds until the next probe is admitted (0.0 when closed or due)."""
        if not self.is_open():
            return 0.0
        return max(0.0, self._open_until - self._now())

    def allows_write(self) -> bool:
        """True when the caller should attempt the write.

        Closed: always. Open: only inside the probe window that opens when the
        cooldown expires.
        """
        return not self.is_open() or self._in_probe_window()

    def _in_probe_window(self) -> bool:
        """True while the circuit is open and its cooldown has elapsed."""
        return self.is_open() and self._now() >= self._open_until

    def record_success(self) -> Optional[CircuitTransition]:
        """Report a successful write. Returns a transition only on recovery."""
        if self.is_open():
            return self._close() if self._in_probe_window() else None
        self._observe(False)
        return None

    def record_failure(self) -> Optional[CircuitTransition]:
        """Report a failed write. Returns a transition when opening or re-arming.

        A failure reported while the circuit is open but *outside* the probe
        window is a straggler — a write admitted before the circuit tripped, or
        one of a concurrent batch that shared a probe window. It is counted but
        must not advance the backoff schedule, or a single dispatched chunk
        would exhaust the whole schedule at once.
        """
        self._failures_while_open += 1
        if self.is_open():
            return self._rearm() if self._in_probe_window() else None
        self._observe(True)
        if self._should_open():
            return self._open()
        return None

    def _observe(self, failed: bool) -> None:
        """Fold one closed-circuit outcome into the rolling window."""
        self._recent.append(failed)
        if len(self._recent) > self._window:
            self._recent.popleft()

    def _should_open(self) -> bool:
        """True once the recent window holds enough failures to judge it down."""
        if len(self._recent) < self._min_samples:
            return False
        return sum(self._recent) / len(self._recent) > self._threshold

    def _open(self) -> CircuitTransition:
        """Trip the circuit and start the backoff at the base cooldown."""
        now = self._now()
        failures = sum(self._recent)
        self._cooldown = self._base_cooldown
        self._open_until = now + self._cooldown
        self._opened_at = now
        self._consecutive_trips = 0
        self._failures_while_open = failures
        self._recent.clear()
        return CircuitTransition(
            name=self._name,
            opened=True,
            cooldown=self._cooldown,
            failures=failures,
        )

    def _rearm(self) -> CircuitTransition:
        """A half-open probe failed: double the cooldown, capped."""
        self._consecutive_trips += 1
        self._cooldown = min(self._cooldown * 2, self._max_cooldown)
        self._open_until = self._now() + self._cooldown
        return CircuitTransition(
            name=self._name,
            probe_failed=True,
            cooldown=self._cooldown,
            failures=self._failures_while_open,
        )

    def _close(self) -> CircuitTransition:
        """A half-open probe succeeded: reset to the closed, healthy state."""
        transition = CircuitTransition(
            name=self._name,
            recovered=True,
            failures=self._failures_while_open,
            downtime=self._now() - self._opened_at,
        )
        self._recent.clear()
        self._open_until = 0.0
        self._cooldown = self._base_cooldown
        self._consecutive_trips = 0
        self._failures_while_open = 0
        self._opened_at = 0.0
        return transition

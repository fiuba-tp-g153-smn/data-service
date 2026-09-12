"""Exponential-backoff throttle for log messages that repeat under an outage."""

import time
from dataclasses import dataclass
from typing import Callable, Dict, Optional

DEFAULT_BASE_INTERVAL = 1.0
DEFAULT_MAX_INTERVAL = 30.0
DEFAULT_FACTOR = 2.0


@dataclass
class _KeyState:
    """Per-key backoff position. Mutable: updated on every check."""

    next_at: float
    interval: float
    suppressed: int = 0


class BackoffLogThrottle:
    """Decide whether a repeating log line should be emitted this time.

    For failures that arrive in a tight loop with no retry cadence of their own
    — a per-request read path, say — there is nothing to pace, so the log line
    itself is what has to back off. The first occurrence logs immediately, then
    the gap doubles (1s, 2s, 4s …) up to `max_interval`, so a sustained outage
    costs a bounded handful of lines per minute however fast the events arrive.

    `check` returns the number of occurrences suppressed since the last emitted
    line (0 on the first), or None to stay silent — so the caller can report the
    true scale in the line it does write. `reset` re-arms a key, making the next
    occurrence log immediately.

    Keys must be low-cardinality (a provider id, an exception class name): each
    one holds a small state entry for the process's lifetime. Anything
    per-request or per-tile would grow without bound.
    """

    def __init__(
        self,
        *,
        base_interval: float = DEFAULT_BASE_INTERVAL,
        max_interval: float = DEFAULT_MAX_INTERVAL,
        factor: float = DEFAULT_FACTOR,
        time_fn: Callable[[], float] = time.monotonic,
    ):
        self._base = base_interval
        self._max = max_interval
        self._factor = factor
        self._now = time_fn
        self._states: Dict[str, _KeyState] = {}

    def check(self, key: str) -> Optional[int]:
        """Return the suppressed count if this occurrence should log, else None."""
        now = self._now()
        state = self._states.get(key)
        if state is None:
            self._states[key] = _KeyState(next_at=now + self._base, interval=self._base)
            return 0
        if now < state.next_at:
            state.suppressed += 1
            return None
        suppressed = state.suppressed
        state.suppressed = 0
        state.interval = min(state.interval * self._factor, self._max)
        state.next_at = now + state.interval
        return suppressed

    def reset(self, key: str) -> None:
        """Forget a key's backoff so its next occurrence logs immediately."""
        self._states.pop(key, None)

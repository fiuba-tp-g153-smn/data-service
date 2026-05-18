"""HTTP client for the SMN weather API with JWT token cache + refresh-on-401."""

import asyncio
import logging
import time
from typing import Any, List, Optional

import httpx

logger = logging.getLogger(__name__)


class SmnApiError(Exception):
    """SMN API call failed after retries / refresh attempts."""


class SmnApiClient:
    """
    Thin async client around the SMN `/weather/station` + `/api-token/auth` endpoints.

    Owns a single `httpx.AsyncClient` (connection pool) and an in-memory JWT
    cache. On every `fetch_current_weather_stations()` call the client uses the
    cached token; on a 401 it refreshes once and retries. The cache TTL bounds
    how often we proactively re-auth even when SMN hasn't rotated the token.

    Not thread-safe across event loops; meant to be a process-wide singleton
    instantiated in the FastAPI `lifespan`.
    """

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        timeout_seconds: float,
        max_retries: int,
        token_cache_ttl_seconds: int,
        token_settling_delay_seconds: float = 0.0,
        user_agent: str = "curl/8.10.1",
        log_requests: bool = False,
    ):
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        self._base_url = base_url.rstrip("/")
        self._username = username
        self._password = password
        self._max_retries = max_retries
        self._token_cache_ttl = token_cache_ttl_seconds
        # Post-mint pause so SMN's auth backend can propagate the new JWT to
        # its validation tier before we hit /weather/station. Opt-in: default
        # 0 keeps behavior unchanged for callers that don't need this.
        self._settling_delay = token_settling_delay_seconds
        # `follow_redirects=True` because SMN occasionally returns 3xx on its
        # API endpoints (observed http→https on the auth endpoint). Without it
        # httpx treats the redirect as a final response and the request fails
        # with a non-2xx status.
        #
        # Client-level headers mimic what `curl --silent` sends so any
        # WAF/bot fingerprinting on User-Agent treats us like a real client.
        # Per-request `headers=...` calls (e.g. `Accept: application/json` on
        # /weather/station) merge with these and override only the keys they
        # specify, so the UA is always preserved.
        event_hooks: dict[str, list] = {}
        if log_requests:
            event_hooks["request"] = [self._log_outbound_request]
            logger.warning(
                "SMN request logging is ON — full URL, headers (including "
                "JWT), and request bodies (including auth POST with "
                "username/password) will be written to logs. Disable in prod."
            )
        self._client = httpx.AsyncClient(
            timeout=timeout_seconds,
            follow_redirects=True,
            headers={"User-Agent": user_agent, "Accept": "*/*"},
            event_hooks=event_hooks,
        )
        self._token: Optional[str] = None
        self._token_minted_at: float = 0.0
        # Serialize concurrent refreshes so a burst of 401s mints a single new
        # token instead of N parallel /api-token/auth calls.
        self._refresh_lock = asyncio.Lock()

    async def close(self) -> None:
        """Release the underlying httpx pool."""
        await self._client.aclose()

    @staticmethod
    async def _log_outbound_request(request: httpx.Request) -> None:
        """httpx event hook: dump the fully-prepared outgoing request.

        Intentionally NOT redacted — operators enable this exactly when they
        need to compare what we're putting on the wire against a known-good
        `curl` invocation. The startup banner warns about the sensitivity.
        """
        body = ""
        if request.content:
            try:
                body = request.content.decode("utf-8")
            except UnicodeDecodeError:
                body = f"<{len(request.content)} bytes, non-utf-8>"
        logger.info(
            "SMN outbound | %s %s\n  headers=%s\n  body=%s",
            request.method,
            request.url,
            dict(request.headers),
            body or "<empty>",
        )

    async def fetch_current_weather_stations(self) -> List[dict[str, Any]]:
        """
        GET {base}/weather/station and return the parsed JSON array.

        Raises `SmnApiError` on any non-recoverable failure (auth still bad
        after a refresh, retries exhausted, malformed payload).
        """
        token = await self._get_token()
        try:
            return await self._get_stations(token)
        except _Unauthorized as first:
            logger.info(
                "SMN token rejected mid-flight (body=%r); refreshing and retrying once",
                first.body,
            )
            # Pass the rejected token in so the refresh lock's double-check
            # doesn't hand the same dead token back to a stampede of 401s.
            token = await self._refresh_token(invalidate=token)
            try:
                return await self._get_stations(token)
            except _Unauthorized as second:
                # Second 401 means the fresh token was ALSO rejected — the
                # credentials are wrong, or the account doesn't have access
                # to this endpoint. Surface a real error AND the upstream
                # response body so it's diagnosable from a log line.
                raise SmnApiError(
                    f"SMN /weather/station rejected the freshly-minted token "
                    f"(401 after refresh). Upstream body: {second.body!r}. "
                    f"Check SMN_API_USERNAME / SMN_API_PASSWORD and the "
                    f"account's API access."
                ) from second

    async def _get_stations(self, token: str) -> List[dict[str, Any]]:
        url = f"{self._base_url}/weather/station"
        # SMN expects the JWT on the standard `Authorization` header with the
        # `JWT ` scheme prefix (Django REST framework convention). Sending it
        # on `api_key` returns 401 even with a valid token.
        headers = {
            "Authorization": f"JWT {token}",
            "Accept": "application/json",
        }
        last_exc: Optional[Exception] = None
        for attempt in range(1, self._max_retries + 1):
            try:
                response = await self._client.get(url, headers=headers)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_exc = exc
                logger.warning(
                    "SMN /weather/station network error (attempt %d/%d): %s",
                    attempt,
                    self._max_retries,
                    exc,
                )
                await asyncio.sleep(min(2 ** (attempt - 1), 10))
                continue

            if response.status_code == 401:
                # Capture the upstream body so callers can surface what SMN
                # actually said (truncated to keep logs bounded if upstream
                # returns HTML).
                raise _Unauthorized(response.text[:500])
            if 500 <= response.status_code < 600:
                last_exc = SmnApiError(
                    f"SMN /weather/station upstream {response.status_code}"
                )
                logger.warning(
                    "SMN /weather/station %d (attempt %d/%d)",
                    response.status_code,
                    attempt,
                    self._max_retries,
                )
                await asyncio.sleep(min(2 ** (attempt - 1), 10))
                continue
            if response.status_code != 200:
                raise SmnApiError(
                    f"SMN /weather/station unexpected status "
                    f"{response.status_code}: {response.text[:200]}"
                )

            payload = response.json()
            if not isinstance(payload, list):
                raise SmnApiError(
                    f"SMN /weather/station returned non-list payload: "
                    f"{type(payload).__name__}"
                )
            return payload

        raise SmnApiError(
            f"SMN /weather/station failed after {self._max_retries} attempts: "
            f"{last_exc}"
        )

    async def _get_token(self) -> str:
        """Return a cached token if still fresh, else mint a new one."""
        if self._token and (time.monotonic() - self._token_minted_at) < self._token_cache_ttl:
            return self._token
        return await self._refresh_token()

    async def _refresh_token(self, invalidate: Optional[str] = None) -> str:
        """Mint a new JWT via POST /api-token/auth; serialize concurrent refreshes.

        `invalidate` carries the token that was just rejected by upstream. When
        supplied, the double-check only reuses the cached token if it is
        *different* from the rejected one (i.e. another waiter already refreshed
        past it). Without this, a 401 stampede would each hit the lock, see
        the still-fresh cached token, and hand the dead one back.
        """
        async with self._refresh_lock:
            if (
                self._token
                and (invalidate is None or self._token != invalidate)
                and (time.monotonic() - self._token_minted_at) < self._token_cache_ttl
            ):
                return self._token

            url = f"{self._base_url}/api-token/auth"
            try:
                response = await self._client.post(
                    url,
                    json={"username": self._username, "password": self._password},
                    headers={"Accept": "application/json"},
                )
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                raise SmnApiError(f"SMN /api-token/auth network error: {exc}") from exc

            if response.status_code != 200:
                raise SmnApiError(
                    f"SMN /api-token/auth returned {response.status_code}: "
                    f"{response.text[:200]}"
                )

            payload = response.json()
            token = payload.get("token") if isinstance(payload, dict) else None
            if not isinstance(token, str) or not token:
                raise SmnApiError(
                    f"SMN /api-token/auth response missing 'token' field: "
                    f"{type(payload).__name__}"
                )

            self._token = token
            self._token_minted_at = time.monotonic()
            logger.info(
                "SMN token generated successfully (length=%d chars)", len(token)
            )
            # Optional settling pause INSIDE the refresh lock so any concurrent
            # waiters that piled up on a 401-stampede all pay the wait once
            # (they exit the lock with the same already-aged token). Guarded
            # so the default zero case doesn't even incur a scheduler hop.
            if self._settling_delay > 0:
                logger.info(
                    "Waiting %.2fs for SMN to propagate the new token",
                    self._settling_delay,
                )
                await asyncio.sleep(self._settling_delay)
            return token


class _Unauthorized(Exception):
    """Internal marker raised on 401 so the caller can trigger one refresh.

    Carries the upstream response body (truncated) so callers can surface
    what SMN actually returned — important when the 401 is the *second* one
    in a row (no further retry; the body is the only diagnostic we have).
    """

    def __init__(self, body: str = ""):
        super().__init__(body)
        self.body = body

"""GZip compression limited to requests under given path prefixes."""

from typing import Tuple

from starlette.middleware.gzip import GZipMiddleware
from starlette.types import ASGIApp, Receive, Scope, Send


class PathScopedGZipMiddleware:  # pylint: disable=too-few-public-methods
    """Apply Starlette's ``GZipMiddleware`` only to paths under ``prefixes``.

    Compression is opt-in per route family so the hot map paths (tiles,
    availability, GeoJSON) keep exactly their current responses while bulky
    JSON such as the status-dashboard metrics gets compressed. Everything else
    (``Accept-Encoding`` negotiation, ``Vary``, the size threshold, skipping
    already-compressed media types) is Starlette's own behaviour. Requests
    outside the prefixes go straight to the app with no wrapper in between.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        prefixes: Tuple[str, ...],
        minimum_size: int = 1024,
        compresslevel: int = 6,
    ) -> None:
        self._app = app
        self._prefixes = prefixes
        self._gzip = GZipMiddleware(
            app, minimum_size=minimum_size, compresslevel=compresslevel
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http" and scope["path"].startswith(self._prefixes):
            await self._gzip(scope, receive, send)
            return
        await self._app(scope, receive, send)

"""Sync observability endpoint."""

from fastapi import APIRouter, Depends, status

from clients.redis_client import RedisClient
from dependencies import get_redis_client
from models.sync import SyncStatusResponse

router = APIRouter(prefix="/sync", tags=["Sync"])


@router.get(
    "/status",
    status_code=status.HTTP_200_OK,
    summary="Get Sync Status",
    response_description="Returns current sync service status and metrics",
    response_model=SyncStatusResponse,
)
async def get_sync_status(
    redis_client: RedisClient = Depends(get_redis_client),
):
    """
    Get the current status of the background sync services.

    Returns metrics including last sync time, duration, download counts,
    error counts, and tileset counts for both satellite and radar data.
    """
    raw = await redis_client.get_sync_status()

    if not raw:
        return SyncStatusResponse()

    def _get_float(key: str):
        v = raw.get(key)
        return float(v) if v else None

    def _get_int(key: str):
        v = raw.get(key)
        return int(v) if v else None

    return SyncStatusResponse(
        is_running=raw.get("is_running", "false") == "true",
        last_sync_start=_get_float("last_sync_start"),
        last_sync_end=_get_float("last_sync_end"),
        last_sync_duration_ms=_get_int("last_sync_duration_ms"),
        last_sync_downloaded=_get_int("last_sync_downloaded"),
        last_sync_errors=_get_int("last_sync_errors"),
        consecutive_failures=int(raw.get("consecutive_failures", "0")),
        total_cycles=int(raw.get("total_cycles", "0")),
        satellite_tilesets_count=int(raw.get("satellite_tilesets_count", "0")),
        radar_tilesets_count=int(raw.get("radar_tilesets_count", "0")),
    )

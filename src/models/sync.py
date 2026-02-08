"""Sync observability models."""

from typing import Optional
from pydantic import BaseModel


class SyncStatusResponse(BaseModel):
    """Response for sync status endpoint."""

    is_running: bool = False
    last_sync_start: Optional[float] = None
    last_sync_end: Optional[float] = None
    last_sync_duration_ms: Optional[int] = None
    last_sync_downloaded: Optional[int] = None
    last_sync_errors: Optional[int] = None
    consecutive_failures: int = 0
    total_cycles: int = 0
    satellite_tilesets_count: int = 0
    radar_tilesets_count: int = 0

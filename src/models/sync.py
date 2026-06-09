"""Sync observability models."""

from typing import List, Optional

from pydantic import BaseModel

# The products that each run their own independent background sync loop.
SYNC_DOMAINS = ["satellite", "radar", "ecmwf_tp", "ecmwf_mslp", "wrf"]


class DomainSyncStatus(BaseModel):
    """Live status for a single product's sync loop."""

    domain: str
    is_running: bool = False
    last_sync_start: Optional[float] = None
    last_sync_end: Optional[float] = None
    last_sync_duration_ms: Optional[int] = None
    last_sync_downloaded: Optional[int] = None
    last_sync_errors: Optional[int] = None
    consecutive_failures: int = 0
    total_cycles: int = 0


class SyncStatusResponse(BaseModel):
    """Per-product sync status. Each product syncs on its own independent loop."""

    any_running: bool = False
    domains: List[DomainSyncStatus] = []

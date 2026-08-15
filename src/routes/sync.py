"""Sync observability endpoint."""

from typing import Optional

from fastapi import APIRouter, Depends, status

from clients.redis_client import RedisClient
from dependencies import get_redis_client
from models.sync import SYNC_DOMAINS, DomainSyncStatus, SyncStatusResponse

router = APIRouter(prefix="/sync", tags=["Sync"])


def _build_domain_status(domain: str, raw: dict) -> DomainSyncStatus:
    """Build a DomainSyncStatus from a raw `sync:status:{domain}` hash."""

    def _float(key: str) -> Optional[float]:
        v = raw.get(key)
        return float(v) if v else None

    def _int(key: str) -> Optional[int]:
        v = raw.get(key)
        return int(v) if v else None

    return DomainSyncStatus(
        domain=domain,
        is_running=raw.get("is_running", "false") == "true",
        last_sync_start=_float("last_sync_start"),
        last_sync_end=_float("last_sync_end"),
        last_sync_duration_ms=_int("last_sync_duration_ms"),
        last_sync_downloaded=_int("last_sync_downloaded"),
        last_sync_errors=_int("last_sync_errors"),
        consecutive_failures=int(raw.get("consecutive_failures", "0")),
        total_cycles=int(raw.get("total_cycles", "0")),
    )


@router.get(
    "/status",
    status_code=status.HTTP_200_OK,
    summary="Get Sync Status",
    response_description="Per-product background sync status",
    response_model=SyncStatusResponse,
)
async def get_sync_status(
    redis_client: RedisClient = Depends(get_redis_client),
):
    """
    Get the live status of every product's background sync loop.

    Each product (satellite, radar, ECMWF-TP, ECMWF-MSLP, WRF, GFS) syncs on its
    own independent loop, so the response is a per-domain list plus an
    `any_running` rollup.
    """
    domains = []
    for domain in SYNC_DOMAINS:
        raw = await redis_client.get_domain_sync_status(domain)
        domains.append(_build_domain_status(domain, raw))

    return SyncStatusResponse(
        any_running=any(d.is_running for d in domains),
        domains=domains,
    )

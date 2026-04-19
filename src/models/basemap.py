"""Base map provider response models."""

from typing import List

from pydantic import BaseModel


class BasemapProviderInfo(BaseModel):
    """Information about a single base map provider."""

    id: str
    name: str
    min_zoom: int
    max_zoom: int
    cache_max_zoom: int
    attribution: str


class BasemapProvidersResponse(BaseModel):
    """Response listing available base map providers."""

    providers: List[BasemapProviderInfo]

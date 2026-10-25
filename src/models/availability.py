"""Response model for the bundled product-availability snapshot."""

from typing import List

from pydantic import BaseModel


class ProductAvailabilityResponse(BaseModel):
    """Which products currently have data, for every domain at once.

    `available` holds product API paths, so a client checks the same string it
    would have put in the URL of an individual probe:
    `radar-sinarame/RMA2/dbzh/elev0`, `goes19/abi/c13`, `wrf-arg4k/granizo`.

    `available` is the complete snapshot for the supported products: contributors
    use the same Redis/S3 listing strategies as individual product endpoints.
    A missing path means no data is available through those listings at the time
    of the snapshot, so the caller does not need a follow-up probe per product.
    The service briefly memoises the response and conditional GETs use an ETag.

    `domains` is diagnostic: only domains contributing at least one product are
    included. It is not the configured-domain catalogue or an upstream health
    guarantee. If the bundled request fails, clients may use individual probes.
    """

    available: List[str]
    domains: List[str]

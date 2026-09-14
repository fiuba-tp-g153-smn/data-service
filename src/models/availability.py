"""Response model for the bundled product-availability snapshot."""

from typing import List

from pydantic import BaseModel


class ProductAvailabilityResponse(BaseModel):
    """Which products currently have data, for every domain at once.

    `available` holds product API paths, so a client checks the same string it
    would have put in the URL of an individual probe:
    `radar-sinarame/RMA2/dbzh/elev0`, `goes19/abi/c13`, `wrf-arg4k/granizo`.

    It is a positive assertion and nothing else. A product missing from the
    list has NOT been declared empty: the snapshot is built from Redis indexes,
    which are a cache of S3, and every per-product endpoint falls back to S3
    when its index is cold. Treating absence as "no data" would grey out live
    products for as long as a sync takes to fill in — and permanently under
    `sync_mode=on_demand`, where no sync loop runs. Probe what is missing.

    `domains` is diagnostic: the domains that contributed at least one product.
    Useful for spotting an index that has never been written; not a coverage
    guarantee, and not a licence to read absence as emptiness.
    """

    available: List[str]
    domains: List[str]

"""Response model for the bundled product-availability snapshot."""

from typing import Dict, List

from pydantic import BaseModel


class ProductAvailabilityResponse(BaseModel):
    """Which products currently have data, for every domain at once.

    `products` is keyed by the product's own API path, so a client looks up the
    same string it would have put in the URL of an individual probe:
    `radar-sinarame/RMA2/dbzh/elev0`, `goes19/abi/c13`, `wrf-arg4k/granizo`.

    `domains` lists the leading segments this snapshot actually covers. A key
    absent from `products` means "no data" only when its domain is listed; when
    the domain is missing (an index the sync loop has not filled yet) the
    honest reading is "unknown", and the client should fall back to probing
    that product on its own rather than greying it out.
    """

    products: Dict[str, bool]
    domains: List[str]

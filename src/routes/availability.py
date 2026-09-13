"""Bundled availability endpoint: one answer for every product at once."""

from fastapi import APIRouter, Request, Response, status

from dependencies import settings
from models.availability import ProductAvailabilityResponse
from routes.utils import json_listing_response
from services.product_availability_service import product_availability_service

router = APIRouter(prefix="/products", tags=["Availability"])


@router.get(
    "/availability",
    status_code=status.HTTP_200_OK,
    summary="Availability of every product",
    response_description="Which products currently have data, keyed by product path",
    response_model=ProductAvailabilityResponse,
)
async def get_product_availability(request: Request) -> Response:
    """Report which products have data, so a client asks once instead of ~125 times."""
    snapshot = await product_availability_service.snapshot()
    payload = ProductAvailabilityResponse(
        products=snapshot.products, domains=snapshot.domains
    ).model_dump()
    return json_listing_response(
        payload, request.headers.get("if-none-match"), settings.cache_control_config
    )

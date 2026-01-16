"""General products API endpoints."""

from fastapi import APIRouter, status

from dependencies import logger
from services.base_service import BaseProductService
from models.base import ProductsListResponse

router = APIRouter(prefix="/products", tags=["Products"])


@router.get(
    "",
    status_code=status.HTTP_200_OK,
    summary="List All Products",
    response_description="Returns all available products (satellites, radar, models, etc.)",
    response_model=ProductsListResponse,
)
async def list_products():
    """
    List all available products and their basic information.

    Products include:
    - **goes-19**: GOES-19 Satellite (ABI, GLM instruments)
    - **radar**: Weather Radar Network
    - **numerical-models**: Numerical Weather Prediction Models (coming soon)
    - **emas**: Automatic Weather Stations (coming soon)
    """
    logger.info("Listing all available products")
    return BaseProductService.get_all_products()

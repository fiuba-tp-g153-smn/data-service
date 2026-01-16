"""Routes for weather data endpoints."""

from fastapi import APIRouter, status, HTTPException, Query
from typing import Optional, Dict
from services.weather_service import weather_service
from dependencies import logger
from models.weather import WeatherTimeSlot, WeatherFeature

router = APIRouter(prefix="/weather", tags=["Weather"])


@router.get(
    "/emas",
    status_code=status.HTTP_200_OK,
    summary="Get Weather Stations (EMAs)",
    response_description="Returns weather station data from Weather.com",
    response_model=Dict[str, WeatherTimeSlot],
)
async def get_emas(
    x: int = Query(..., description="Tile coordinate X"),
    y: int = Query(..., description="Tile coordinate Y"),
    lod: int = Query(8, description="Level of detail"),
    time: Optional[list[str]] = Query(
        None, description="Time ranges (format: start-end:value)"
    ),
):
    """
    Get weather stations (EMAs) data from Weather.com Wundermap.

    Example usage:
    - /weather/emas?x=41&y=77&lod=8
    """
    try:
        logger.info(f"Getting EMAs for coordinates x={x}, y={y}, lod={lod}")
        data = await weather_service.get_emas(x=x, y=y, lod=lod, time=time)
        result = {
            slot: WeatherTimeSlot(
                features=[
                    WeatherFeature(**feature)
                    for feature in slot_data.get("features", [])
                ]
            )
            for slot, slot_data in data.items()
        }
        return result
    except Exception as e:
        logger.error(f"Error getting EMAs: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching weather data: {str(e)}",
        )

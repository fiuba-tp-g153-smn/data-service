"""Service for interacting with Weather.com API."""
import httpx
from typing import Optional
from dependencies import logger
from datetime import datetime, timedelta


class WeatherService:
    """Service to fetch weather data from Weather.com API."""
    
    BASE_URL = "https://api2.weather.com/v2/vector-api/products/614/features"
    API_KEY = "REDACTED"
    TILE_SIZE = 512
    
    async def _get_time_ranges_from_info(self, lod: int = 8) -> list[str]:
        """
        Fetch time ranges from the Weather.com /info endpoint for the given LOD.
        
        Args:
            lod: Level of detail (default: 8)
        
        Returns:
            List of time range strings
        """
        info_url = "https://api2.weather.com/v2/vector-api/products/614/info"
        params = {
            "apiKey": self.API_KEY,
            "tile-size": self.TILE_SIZE
        }
        try:
            async with httpx.AsyncClient() as client:
                logger.info("Fetching time ranges from /info endpoint")
                response = await client.get(info_url, params=params, timeout=15.0)
                response.raise_for_status()
                data = response.json()
                # Extrae los time ranges del producto 614
                return data.get("products", {}).get("614", {}).get("time", [])
        except Exception as e:
            logger.error(f"Error fetching time ranges from /info: {e}")
            raise

    async def get_emas(
        self,
        x: int,
        y: int,
        lod: int = 8,
        time: Optional[list[str]] = None
    ) -> dict:
        """
        Get weather stations (EMAs) data from Weather.com.
        Now automatically fetches time ranges from /info if not provided.
        """
        # Si no se pasan time ranges, los obtiene del endpoint /info
        if time is None:
            time_ranges = await self._get_time_ranges_from_info(lod=lod)
        else:
            time_ranges = time

        params = [
            ("x", x),
            ("y", y),
            ("lod", lod),
            ("apiKey", self.API_KEY),
            ("tile-size", self.TILE_SIZE),
            ("stepped", "true"),
        ]
        for time_range in time_ranges:
            params.append(("time", time_range))
        
        try:
            async with httpx.AsyncClient() as client:
                logger.info(f"Fetching weather data for tile ({x}, {y}) at LOD {lod}")
                logger.debug(f"Time ranges: {time_ranges}")
                response = await client.get(self.BASE_URL, params=params, timeout=30.0)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Error fetching weather data: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise

weather_service = WeatherService()
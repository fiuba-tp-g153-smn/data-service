from typing import Any, Dict

# General endpoints
ROOT_RESPONSES: Dict[int | str, Dict[str, Any]] = {
    200: {
        "description": "Service is running correctly",
        "content": {
            "application/json": {
                "example": {"status": "ok", "service": "users-service"}
            }
        },
    }
}

HEALTH_RESPONSES: Dict[int | str, Dict[str, Any]] = {
    200: {
        "description": "Service is running",
        "content": {"application/json": {"example": {"status": "running"}}},
    }
}

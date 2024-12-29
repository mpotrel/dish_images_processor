from fastapi import APIRouter

from .dev import dev_router
from .prod import prod_router


v0_router = APIRouter(prefix="/v0")
v0_router.include_router(prod_router)
v0_router.include_router(dev_router)


__all__ = [
    "v0_router"
]

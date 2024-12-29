from fastapi import FastAPI

from dish_images_processor.routers.dev import dev_router
from dish_images_processor.routers.prod import prod_router


app = FastAPI()
app.include_router(prod_router)
app.include_router(dev_router)

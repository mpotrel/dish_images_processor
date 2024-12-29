from fastapi import FastAPI

from dish_images_processor.routers.v0 import v0_router


app = FastAPI()
app.include_router(v0_router)

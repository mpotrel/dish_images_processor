from fastapi import APIRouter

from dish_images_processor.models.messages import InputImageMessage


prod_router = APIRouter()

@prod_router.post("/preprocess")
async def preprocess_img(input_img: InputImageMessage):
    pass

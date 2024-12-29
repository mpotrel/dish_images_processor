from fastapi import APIRouter
import uuid

from dish_images_processor.dependencies import get_producers
from dish_images_processor.models.messages import InputImageMessage


prod_router = APIRouter()

@prod_router.post("/preprocess")
async def preprocess_img(input_images: list[str]):
    """Process a list of image URLs through the pipeline"""
    job_id = str(uuid.uuid4())

    # Create initial messages for each image
    messages = [
        InputImageMessage(
            job_id=f"{job_id}-{i}",
            image_url=url
        ) for i, url in enumerate(input_images)
    ]

    producers = get_producers()
    producer = producers["background_removal"]
    for message in messages:
        await producer.send_message(message)

    return {"status": "processing", "job_id": job_id}

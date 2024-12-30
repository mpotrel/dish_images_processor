import uuid

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from dish_images_processor.config.logging import get_logger
from dish_images_processor.dependencies import get_producers
from dish_images_processor.models.messages import InputImageMessage

logger = get_logger(__name__)

class ImageUrls(BaseModel):
    images: list[str]

class ProcessingResponse(BaseModel):
    job_id: str
    status: str

prod_router = APIRouter()

@prod_router.post("/preprocess", response_model=ProcessingResponse)
async def preprocess_img(
    image_list: ImageUrls,
    producers: dict = Depends(get_producers)
):
    job_id = str(uuid.uuid4())

    messages = [
        InputImageMessage(
            job_id=f"{job_id}-{i}",
            image_url=url
        ) for i, url in enumerate(image_list.images)
    ]

    try:
        producer = producers["background_removal"]
        for message in messages:
            await producer.send_message(message)

        logger.info(f"Initiated image processing job {job_id} for {len(messages)} images")
    except Exception as e:
        logger.error(f"Failed to process images for job {job_id}: {e}")
        raise

    return ProcessingResponse(
        job_id=job_id,
        status="processing"
    )

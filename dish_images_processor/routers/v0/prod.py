import uuid

from fastapi import APIRouter, Depends, HTTPException

from dish_images_processor.config.logging import get_logger
from dish_images_processor.dependencies import get_producers
from dish_images_processor.models.messages import InputImageMessage
from dish_images_processor.models.preprocess import ImageUrls, ProcessingResponse

logger = get_logger(__name__)

prod_router = APIRouter()

@prod_router.post("/preprocess", response_model=ProcessingResponse)
async def preprocess_img(
    image_list: ImageUrls,
    producers: dict = Depends(get_producers)
) -> ProcessingResponse:
    if not image_list.images:
        raise HTTPException(status_code=422, detail="Image list cannot be empty")
    job_id = str(uuid.uuid4())
    try:
        producer = producers.get('background_removal')
        if not producer:
            raise HTTPException(
                status_code=500,
                detail="Background removal service unavailable"
            )

        messages = [
            InputImageMessage(
                job_id=f"{job_id}-{i}",
                image_url=str(url)  # Convert HttpUrl to str
            ) for i, url in enumerate(image_list.images)
        ]

        for message in messages:
            await producer.send_message(message)

        logger.info(f"Initiated image processing job {job_id} for {len(messages)} images")

        return ProcessingResponse(
            job_id=job_id,
            status="processing"
        )

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Failed to process images for job {job_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

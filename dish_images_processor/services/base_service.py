from typing import Optional

import fal_client

from dish_images_processor.config.settings import get_settings
from dish_images_processor.models.messages import InputImageMessage, OutputImageMessage


class ImageProcessingService:
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.settings = get_settings(service_name)

    async def process(self, message: InputImageMessage) -> Optional[OutputImageMessage]:
        """
        Args:
            message (OutputImageMessage): Message containing the image to process

        Returns:
            Optional[OutputImageMessage]: Processed message
        """
        try:
            arguments = self.settings.arguments.copy()
            arguments["image_url"] = message.image_url
            # Submit upscaling task
            handler = fal_client.submit(
                self.settings.endpoint,
                arguments=arguments
            )

            # Wait for result
            result = handler.get()

            # Update message with upscaled image URL
            return OutputImageMessage(
                job_id=message.job_id,
                image_url=message.image_url,
                created_at=message.created_at,
                processed_url=result["image_url"]
            )

        except Exception as e:
            print(f"{self.service_name} failed for job {message.job_id}: {e}")

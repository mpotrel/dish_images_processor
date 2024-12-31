from typing import Optional

import fal_client

from dish_images_processor.config.logging import get_logger
from dish_images_processor.config.settings import get_settings
from dish_images_processor.models.messages import InputImageMessage, OutputImageMessage
from dish_images_processor.dependencies import get_completed_messages, get_producers
from dish_images_processor.utils.image_validation import is_valid_image

logger = get_logger(__name__)

class ImageProcessingService:
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.settings = get_settings(service_name)

    async def process(self, message_dict: dict) -> Optional[OutputImageMessage]:
        try:
            logger.info(f"Processing message in {self.service_name}: {message_dict}")
            if message_dict.get('processed_by', {}).get(self.service_name, False):
                logger.info(f"Message already processed by {self.service_name}, skipping")
                return

            input_message_params = {
                k: v for k, v in message_dict.items()
                if k in ['job_id', 'image_url', 'created_at', 'unprocessed_image_url']
            }
            if "processed_image_url" in message_dict:
                input_message_params["image_url"] = message_dict["processed_image_url"]
            base_message = InputImageMessage(**input_message_params)

            output_message = await self.request_fal(base_message)
            if output_message is None:
                return
            output_message_dict = output_message.model_dump()
            output_message_dict['processed_by'] = message_dict.get('processed_by', {})
            output_message_dict['processed_by'][self.service_name] = True

            await self._forward_message(OutputImageMessage(**output_message_dict))

            return OutputImageMessage(**output_message_dict)

        except Exception as e:
            logger.error(f"{self.service_name} failed: {e}")
            return

    async def request_fal(self, base_message: InputImageMessage):
        if not await is_valid_image(base_message.image_url):
            logger.error(f"Invalid image at URL: {base_message.image_url}")
            return
        arguments = self.settings.arguments.copy()
        arguments["image_url"] = base_message.image_url
        handler = fal_client.submit(
            self.settings.endpoint,
            arguments=arguments
        )
        result = handler.get()
        # HACK: Needs to be improved if possible
        # The main issue is the lack of standardisation from FAL
        if "image" not in result:
            processed_image_url = result["images"][0]["url"]
        else:
            processed_image_url = result["image"]["url"]
        return OutputImageMessage(
            job_id=base_message.job_id,
            image_url=base_message.image_url,
            created_at=base_message.created_at,
            processed_image_url=processed_image_url,
            unprocessed_image_url=base_message.unprocessed_image_url
        )

    async def _forward_message(self, message: OutputImageMessage):
        try:
            producers = get_producers()
            next_service = self._get_next_service()

            if next_service and next_service in producers:
                producer = producers[next_service]
                await producer.send_message(message)
            else:
                await self._publish_completed_image(message)

        except Exception as e:
            logger.error(f"Message forwarding error: {e}")

    async def _publish_completed_image(self, message: OutputImageMessage):
        try:
            producers = get_producers()
            completed_topic_producer = producers.get('completed_images')

            if completed_topic_producer:
                await completed_topic_producer.send_message(message)
                base_job_id = message.job_id.rsplit('-', 1)[0]
                completed_messages = get_completed_messages()
                if base_job_id not in completed_messages:
                    completed_messages[base_job_id] = []
                completed_messages[base_job_id].append(message)
                logger.info(f"Completed image: {message}")
            else:
                logger.warning("No completed images producer found")

        except Exception as e:
            logger.error(f"Completed image publishing error: {e}")

    def _get_next_service(self) -> Optional[str]:
        service_order = [
            'background_removal',
            'background_generation',
            'hyper_resolution'
        ]

        try:
            current_index = service_order.index(self.service_name)
            return service_order[current_index + 1] if current_index < len(service_order) - 1 else None
        except ValueError:
            logger.error(f"Service {self.service_name} not found in pipeline")
            return

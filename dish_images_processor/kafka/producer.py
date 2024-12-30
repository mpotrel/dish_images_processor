import json
from confluent_kafka import Producer

from dish_images_processor.config.logging import get_logger
from dish_images_processor.config.settings import get_app_settings
from dish_images_processor.models.messages import InputImageMessage, OutputImageMessage

logger = get_logger(__name__)

class MessageProducer:
    def __init__(self, service_name: str, topic: str):
        self.settings = get_app_settings()
        self.service_name = service_name
        self.topic = topic

        self.producer = Producer({
            "bootstrap.servers": self.settings.KAFKA_BOOTSTRAP_SERVERS,
            "client.id": f"{service_name}_producer",
            "acks": "all",
            "delivery.timeout.ms": 10000,
            "request.timeout.ms": 5000,
        })

    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Message delivery failed: {err}")

    async def send_message(self, message: InputImageMessage | OutputImageMessage):
        try:
            message_dict = message.model_dump()

            self.producer.produce(
                self.topic,
                key=str(message_dict['job_id']).encode('utf-8'),
                value=json.dumps(message_dict).encode("utf-8"),
                on_delivery=self.delivery_report
            )

            delivered = self.producer.flush(timeout=5.0)
            if delivered > 0:
                logger.info(f"Successfully delivered message to {self.topic}")
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            raise

    def close(self):
        try:
            remaining = len(self.producer)
            if remaining > 0:
                self.producer.flush()
            self.producer.close()
        except Exception as e:
            logger.error(f"Error closing producer: {e}")

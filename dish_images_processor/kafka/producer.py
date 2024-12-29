import json
from typing import Dict, Any

from confluent_kafka import Producer

from dish_images_processor.config.settings import get_app_settings


class ImageProcessor:
    def __init__(self):
        self.settings = get_app_settings()
        self.producer = Producer(
            {
                "bootstrap.servers": self.settings.KAFKA_BOOTSTRAP_SERVERS,
                "client.id": "image_processor",
            }
        )

    def delivery_report(self, err: Any, msg: Any) -> None:
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def send_message(self, topic: str, message: Dict) -> None:
        try:
            self.producer.produce(
                topic,
                value=json.dumps(message).encode("utf-8"),
                callback=self.delivery_report,
            )
            self.producer.poll(0)  # Trigger delivery reports
        except Exception as e:
            print(f"Failed to send message: {e}")

    def close(self) -> None:
        self.producer.flush()

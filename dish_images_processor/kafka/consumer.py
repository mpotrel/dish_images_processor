import json
from typing import Callable

from confluent_kafka import Consumer, KafkaError

from dish_images_processor.config.settings import get_settings
from dish_images_processor.utils.concurrency import ConcurrencyLimiter


class ImageProcessorConsumer:
    def __init__(
        self,
        topic: str,
        process_message: Callable,
        group_id: str,
        limiter: ConcurrencyLimiter,
    ):
        self.settings = get_settings()
        self.topic = topic
        self.process_message = process_message
        self.limiter = limiter
        self.running = True

        self.consumer = Consumer(
            {
                "bootstrap.servers": self.settings.KAFKA_BOOTSTRAP_SERVERS,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
            }
        )

        self.consumer.subscribe([topic])

    async def start(self):
        while self.running:
            msg = self.consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Consumer error: {msg.error()}")
                continue

            try:
                value = json.loads(msg.value().decode("utf-8"))
                # Use the concurrency limiter
                async with self.limiter.limit():
                    await self.process_message(value)
            except Exception as e:
                print(f"Error processing message: {e}")

    def stop(self):
        self.running = False
        self.consumer.close()

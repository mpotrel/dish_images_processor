import asyncio
from confluent_kafka import Consumer, KafkaError
from typing import Callable
import json

from dish_images_processor.config.settings import get_app_settings
from dish_images_processor.utils.concurrency import ConcurrencyLimiter

class MessageConsumer:
    def __init__(
        self,
        topic: str,
        process_message: Callable,
        group_id: str,
        limiter: ConcurrencyLimiter,
    ):
        self.settings = get_app_settings()
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
        loop = asyncio.get_running_loop()
        while self.running:
            try:
                # Use run_in_executor to make poll non-blocking
                msg = await loop.run_in_executor(None, self.consumer.poll, 1.0)

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    print(f"Consumer error: {msg.error()}")
                    continue

                try:
                    value = json.loads(msg.value().decode("utf-8"))
                    async with self.limiter.limit():
                        await self.process_message(value)
                except Exception as e:
                    print(f"Error processing message: {e}")

                # Add a small delay to prevent tight loop
                await asyncio.sleep(0.1)

            except Exception as e:
                print(f"Error in consumer loop: {e}")
                await asyncio.sleep(1)

    def stop(self):
        self.running = False
        self.consumer.close()

import asyncio
import json
from typing import Callable, Any

from confluent_kafka import Consumer
from confluent_kafka.error import KafkaError

from dish_images_processor.config.logging import get_logger
from dish_images_processor.config.settings import get_app_settings
from dish_images_processor.utils.concurrency import ConcurrencyLimiter

logger = get_logger(__name__)

class MessageConsumer:
    def __init__(
        self,
        topic: str,
        process_message: Callable[[Any], Any],
        group_id: str,
        limiter: ConcurrencyLimiter
    ):
        self.settings = get_app_settings()
        self.consumer_config = {
            "bootstrap.servers": self.settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
            "max.poll.interval.ms": "300000",
        }
        self.topic = topic
        self.process_message = process_message
        self.limiter = limiter
        self.running = False

        # Instantiate consumer during initialization
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([self.topic])

    async def start(self):
        self.running = True
        loop = asyncio.get_event_loop()

        try:
            while self.running:
                result = await loop.run_in_executor(None, self._poll_message)

                if result is None:
                    await asyncio.sleep(0.1)
                    continue

                msg, value = result

                if isinstance(msg, KafkaError):
                    continue

                try:
                    await self._process_with_limiter(value)
                    self.consumer.commit(msg)
                except Exception as e:
                    logger.error(f"Message processing error: {e}")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self.stop()

    def _poll_message(self) -> tuple | None:
        try:
            msg = self.consumer.poll(1.0)

            if msg is None:
                return

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    return
                return msg, msg.error()

            try:
                value = json.loads(msg.value().decode("utf-8"))
                return msg, value
            except Exception:
                return
        except Exception:
            return

    async def _process_with_limiter(self, message: Any):
        async with await self.limiter.limit():
            await self.process_message(message)

    def stop(self):
        self.running = False
        try:
            self.consumer.close()
        except Exception as e:
            logger.error(f"Consumer close error: {e}")

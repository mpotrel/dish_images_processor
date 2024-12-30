import asyncio
import json
import os
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
        logger.info(f"Consumer {group_id} subscribed to topic: {topic}")

    async def start(self):
        self.running = True
        loop = asyncio.get_event_loop()
        logger.info(f"Consumer starting to poll topic: {self.topic}")
        try:
            while self.running:
                result = await loop.run_in_executor(None, self._poll_message)
                if result is None:
                    await asyncio.sleep(0.1)
                    continue

                msg, value = result
                if isinstance(msg, KafkaError):
                    continue
                asyncio.create_task(self._process_message(msg, value))
            await asyncio.Event().wait()
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self.stop()

    async def _process_message(self, msg, value):
        logger.info(f"Finished processing for job {value.get('job_id')}")
        try:
            async with await self.limiter.limit():
                if os.getenv("DEBUG_MODE") == "true":
                    await asyncio.sleep(2)
                await self.process_message(value)

            self.consumer.commit(msg)
        except Exception as e:
            logger.error(f"Message processing error: {e}")

    def _poll_message(self) -> tuple | None:
        try:
            msg = self.consumer.poll(1.0)
            if msg is None:
                return
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    return
                logger.info(f"Error polling from {self.topic}: {msg.error()}")
                return msg, msg.error()

            logger.info(f"Raw message received from {self.topic}: {msg.value()}")
            try:
                value = json.loads(msg.value().decode("utf-8"))
                return msg, value
            except Exception as e:
                logger.error(f"Error decoding message from {self.topic}: {e}")
                return
        except Exception as e:
            logger.error(f"Error polling from {self.topic}: {e}")
            return


    def stop(self):
        self.running = False
        try:
            self.consumer.close()
        except Exception as e:
            logger.error(f"Consumer close error: {e}")

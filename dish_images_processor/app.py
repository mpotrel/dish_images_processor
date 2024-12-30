import asyncio

from fastapi import FastAPI

from dish_images_processor.config.logging import configure_logging, get_logger
from dish_images_processor.config.settings import get_app_settings
from dish_images_processor.dependencies import get_producers
from dish_images_processor.kafka.consumer import MessageConsumer
from dish_images_processor.kafka.topics import TOPICS, TopicManager
from dish_images_processor.routers.v0 import v0_router
from dish_images_processor.services.base_service import ImageProcessingService
from dish_images_processor.utils.concurrency import ConcurrencyLimiter

configure_logging()
logger = get_logger(__name__)

app = FastAPI(title="Dish Images Processor")

settings = get_app_settings()

# Exclude completed images service from processing services
services: dict[str, ImageProcessingService] = {
    service_name: ImageProcessingService(service_name)
    for service_name in TOPICS.keys() if "completed" not in service_name
}

limiters: dict[str, ConcurrencyLimiter] = {
    service_name: ConcurrencyLimiter(settings.MAX_CONCURRENT_REQUESTS)
    for service_name in services.keys()
}

producers = get_producers()

consumers: dict[str, MessageConsumer] = {
    service_name: MessageConsumer(
        topic=TOPICS[service_name]["input"],
        process_message=service.process,
        group_id=f"{service_name}-group",
        limiter=limiters[service_name],
    )
    for service_name, service in services.items()
}

app.include_router(v0_router)

@app.on_event("startup")
async def startup_event():
    try:
        topic_manager = TopicManager()
        await asyncio.get_event_loop().run_in_executor(None, topic_manager.ensure_topics_exist)
        logger.info("Successfully created Kafka topics")

        for service_name, consumer in consumers.items():
            asyncio.create_task(consumer.start())
            logger.info(f"Started consumer for {service_name}")

        logger.info("Application startup completed")
    except Exception as e:
        logger.error(f"Startup procedure failed: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    for service_name, consumer in consumers.items():
        consumer.stop()
        logger.info(f"Stopped consumer for {service_name}")

    for producer in producers.values():
        producer.close()

    logger.info("Kafka producers closed")

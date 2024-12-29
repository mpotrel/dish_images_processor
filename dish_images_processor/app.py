import asyncio

from fastapi import FastAPI

from dish_images_processor.config.settings import get_app_settings
from dish_images_processor.kafka.consumer import MessageConsumer
from dish_images_processor.kafka.producer import MessageProducer
from dish_images_processor.kafka.topics import TOPICS
from dish_images_processor.routers.v0 import v0_router
from dish_images_processor.services.base_service import ImageProcessingService
from dish_images_processor.utils.concurrency import ConcurrencyLimiter


app = FastAPI(title="Dish Images Processor")

settings = get_app_settings()
services = {
    service_name: ImageProcessingService(service_name)
    for service_name in TOPICS.keys()
}
limiters = {
    service_name: ConcurrencyLimiter(settings.MAX_CONCURRENT_REQUESTS)
    for service_name in services.keys()
}

producers = {
    service_name: MessageProducer(
        service_name=service_name,
        input_topic=topics["input"],
        output_topic=topics["output"]
    )
    for service_name, topics in TOPICS.items()
}
consumers = {
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
    """Start Kafka consumers on application startup"""
    for service_name, consumer in consumers.items():
        asyncio.create_task(consumer.start())
        print(f"Started consumer for {service_name}")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup resources on application shutdown"""
    for service_name, consumer in consumers.items():
        consumer.stop()
        print(f"Stopped consumer for {service_name}")
    for producer in producers.values():
        producer.close()
    print("Closed Kafka producer")

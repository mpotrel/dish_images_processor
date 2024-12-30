import asyncio

from fastapi import FastAPI

from dish_images_processor.config.settings import get_app_settings
from dish_images_processor.dependencies import get_producers
from dish_images_processor.kafka.consumer import MessageConsumer
from dish_images_processor.kafka.topics import TOPICS, TopicManager
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

producers = get_producers()
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
    """Initialize Kafka topics and start consumers on application startup"""
    try:
        # Ensure Kafka topics exist
        topic_manager = TopicManager()
        try:
            await asyncio.get_event_loop().run_in_executor(None, topic_manager.ensure_topics_exist)
        except Exception as e:
            print(f"Warning: Failed to create topics: {e}")
            pass

        # Start Kafka consumers
        for service_name, consumer in consumers.items():
            try:
                asyncio.create_task(consumer.start())
                print(f"Started consumer for {service_name}")
            except Exception as e:
                print(f"Warning: Failed to start consumer {service_name}: {e}")
                continue

        print("Application startup completed")
    except Exception as e:
        print(f"Warning: Startup procedure encountered errors: {e}")
        pass

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup resources on application shutdown"""
    for service_name, consumer in consumers.items():
        consumer.stop()
        print(f"Stopped consumer for {service_name}")
    for producer in producers.values():
        producer.close()
    print("Closed Kafka producer")

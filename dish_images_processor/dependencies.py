from dish_images_processor.kafka.producer import MessageProducer
from dish_images_processor.kafka.topics import TOPICS


producers = {
    service_name: MessageProducer(
        service_name=service_name,
        topic=topics["input"] if "input" in topics else service_name
    )
    for service_name, topics in TOPICS.items()
}

# Ensure completed images producer is separate
if "completed_images" in TOPICS:
    producers["completed_images"] = MessageProducer(
        service_name="completed_images",
        topic=TOPICS["completed_images"]["input"]
    )

def get_producers():
    return producers

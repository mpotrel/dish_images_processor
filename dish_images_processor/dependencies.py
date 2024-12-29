from dish_images_processor.kafka.producer import MessageProducer
from dish_images_processor.kafka.topics import TOPICS


producers = {
    service_name: MessageProducer(
        service_name=service_name,
        input_topic=topics["input"],
        output_topic=topics["output"]
    )
    for service_name, topics in TOPICS.items()
}

def get_producers():
    return producers

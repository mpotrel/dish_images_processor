from confluent_kafka.admin import AdminClient, NewTopic

from dish_images_processor.config.logging import get_logger
from dish_images_processor.config.settings import get_app_settings

logger = get_logger(__name__)

# TODO: Make this a yaml file to be able to change this without deployment
TOPICS = {
    "background_removal": {
        "input": "background-removal-input",
        "output": "background-generation-input"
    },
    "background_generation": {
        "input": "background-generation-input",
        "output": "hyper-resolution-input"
    },
    "hyper_resolution": {
        "input": "hyper-resolution-input",
        "output": "hyper-resolution-input"
    },
    "completed_images": {
        "input": "completed-images"
    }
}

class TopicManager:
    def __init__(self):
        self.settings = get_app_settings()
        self.admin_client = AdminClient({
            "bootstrap.servers": self.settings.KAFKA_BOOTSTRAP_SERVERS
        })

    def get_existing_topics(self) -> set:
        topics_list = self.admin_client.list_topics(timeout=10)
        return set(t.topic for t in iter(topics_list.topics.values()))

    def get_required_topics(self) -> list[str]:
        topics = set()
        for service_config in TOPICS.values():
            topics.add(service_config["input"])
            if "output" in service_config:
                topics.add(service_config["output"])
        return list(topics)

    def create_topics(self, topics: list[str], num_partitions: int = 1, replication_factor: int = 1):
        existing_topics = self.get_existing_topics()
        new_topics = [
            NewTopic(
                topic,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            ) for topic in topics if topic not in existing_topics
        ]

        if new_topics:
            futures = self.admin_client.create_topics(new_topics)
            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f"Topic {topic} created successfully")
                except Exception as e:
                    logger.error(f"Failed to create topic {topic}: {e}")

    def ensure_topics_exist(self):
        required_topics = self.get_required_topics()
        self.create_topics(required_topics)

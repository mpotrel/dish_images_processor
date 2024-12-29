from confluent_kafka.admin import AdminClient, NewTopic

from dish_images_processor.config.settings import get_app_settings


# TODO: Make this a yaml file so it can be changed without deployments
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
        "output": "completed-images"
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
            topics.add(service_config["output"])
        return list(topics)

    def create_topics(self, topics: list[str], num_partitions: int = 1, replication_factor: int = 1):
        existing_topics = self.get_existing_topics()
        new_topics = []

        for topic in topics:
            if topic not in existing_topics:
                new_topics.append(NewTopic(
                    topic,
                    num_partitions=num_partitions,
                    replication_factor=replication_factor
                ))

        if new_topics:
            futures = self.admin_client.create_topics(new_topics)

            for topic, future in futures.items():
                try:
                    future.result()
                    print(f"Topic {topic} created successfully")
                except Exception as e:
                    print(f"Failed to create topic {topic}: {e}")

    def ensure_topics_exist(self):
        required_topics = self.get_required_topics()
        self.create_topics(required_topics)

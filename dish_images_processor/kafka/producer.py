import json
from typing import Union

from confluent_kafka import Producer

from dish_images_processor.config.settings import get_app_settings
from dish_images_processor.models.messages import InputImageMessage, OutputImageMessage


class MessageProducer:
    def __init__(self, service_name: str, input_topic: str, output_topic: str):
        """
        Initialize a service specific producer

        Args:
            service_name (str): Name of the service
            input_topic (str): Topic to consume messages from
            output_topic (str): Topic to produce processed messages to
        """
        self.settings = get_app_settings()
        self.service_name = service_name
        self.input_topic = input_topic
        self.output_topic = output_topic

        self.producer = Producer({
            "bootstrap.servers": self.settings.KAFKA_BOOTSTRAP_SERVERS,
            "client.id": f"{service_name}_producer",
        })

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"[{self.service_name}] Message delivery failed: {err}")
        else:
            print(f"[{self.service_name}] Message delivered to {msg.topic()} [{msg.partition()}]")

    async def send_message(self, message: Union[InputImageMessage, OutputImageMessage]):
        try:
            message_dict = message.model_dump()
            self.producer.produce(
                self.output_topic,
                value=json.dumps(message_dict).encode("utf-8"),
                callback=self.delivery_report,
            )
            self.producer.poll(0)

        except Exception as e:
            print(f"[{self.service_name}] Failed to send message to topic {self.output_topic}: {e}")

    def close(self):
        self.producer.flush()

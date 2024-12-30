import json
import pytest
from unittest.mock import AsyncMock
from confluent_kafka import KafkaError, Message

from dish_images_processor.kafka.consumer import MessageConsumer
from dish_images_processor.models.messages import InputImageMessage

@pytest.mark.asyncio
async def test_producer_send_message(mock_kafka_producer, mocker):
    test_message = InputImageMessage(
        job_id="test-123",
        image_url="http://example.com/test.jpg"
    )

    mock_producer = mocker.MagicMock()
    mock_kafka_producer.producer = mock_producer
    mock_producer.flush.return_value = 0

    await mock_kafka_producer.send_message(test_message)

    mock_producer.produce.assert_called_once()
    args = mock_producer.produce.call_args
    assert args[0][0] == "test-topic"
    assert args[1]["key"] == b"test-123"

@pytest.mark.asyncio
async def test_producer_send_message_error(mock_kafka_producer, mocker):
    test_message = InputImageMessage(
        job_id="test-123",
        image_url="http://example.com/test.jpg"
    )

    mock_producer = mocker.MagicMock()
    mock_kafka_producer.producer = mock_producer
    mock_producer.produce.side_effect = Exception("Kafka error")

    with pytest.raises(Exception):
        await mock_kafka_producer.send_message(test_message)

@pytest.mark.asyncio
async def test_consumer_process_message(mocker):
    process_message = AsyncMock()
    limiter = mocker.MagicMock()
    limiter.limit = AsyncMock()

    consumer = MessageConsumer(
        topic="test-topic",
        process_message=process_message,
        group_id="test-group",
        limiter=limiter
    )

    message = mocker.MagicMock(spec=Message)
    message.error.return_value = None
    message.value.return_value = json.dumps({
        "job_id": "test-123",
        "image_url": "http://example.com/test.jpg"
    }).encode('utf-8')

    consumer.consumer = mocker.MagicMock()
    consumer.consumer.poll.return_value = message
    consumer.consumer.commit = mocker.MagicMock()

    await consumer._process_with_limiter(json.loads(message.value().decode('utf-8')))

    process_message.assert_called_once()
    assert limiter.limit.called

@pytest.mark.asyncio
async def test_consumer_handle_error(mocker):
    process_message = AsyncMock()
    consumer = MessageConsumer(
        topic="test-topic",
        process_message=process_message,
        group_id="test-group",
        limiter=mocker.MagicMock()
    )

    message = mocker.MagicMock(spec=Message)
    mock_error = mocker.MagicMock(spec=KafkaError)
    mock_error.code.return_value = KafkaError._PARTITION_EOF
    message.error.return_value = mock_error

    consumer.consumer = mocker.MagicMock()
    consumer.consumer.poll.return_value = message

    result = consumer._poll_message()
    assert result is None
    assert not process_message.called

@pytest.mark.asyncio
async def test_producer_prevents_duplicate_message_production(mock_kafka_producer, sample_image_urls):
    # Create a list to track sent messages
    sent_messages = []

    # Mock the producer's produce method to capture messages
    def capture_messages(topic, key, value, on_delivery):
        # Check if this message key (job_id) has already been sent
        existing_message = next((m for m in sent_messages if m['key'] == key), None)
        assert existing_message is None, f"Duplicate message with key {key} attempted to be produced"

        sent_messages.append({
            'topic': topic,
            'key': key,
            'value': value
        })

    mock_kafka_producer.producer.produce.side_effect = capture_messages

    # Simulate sending messages for each image URL
    for url in sample_image_urls['images']:
        test_message = InputImageMessage(
            job_id=f"unique-job-{url.split('/')[-1]}",
            image_url=url
        )
        await mock_kafka_producer.send_message(test_message)

    # Verify the number of unique messages matches the number of input URLs
    assert len(sent_messages) == len(sample_image_urls['images'])

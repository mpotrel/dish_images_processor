import json
import pytest
from unittest.mock import AsyncMock
from confluent_kafka import KafkaError, Message

from dish_images_processor.kafka.consumer import MessageConsumer
from dish_images_processor.kafka.topics import TopicManager
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
    limiter.limit.return_value.__aenter__ = AsyncMock()
    limiter.limit.return_value.__aexit__ = AsyncMock()

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

    message_data = json.loads(message.value().decode('utf-8'))
    await consumer._process_message(message, message_data)

    process_message.assert_called_once()
    assert limiter.limit.called
    assert limiter.limit.return_value.__aenter__.called
    assert limiter.limit.return_value.__aexit__.called

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
    sent_messages = []

    def capture_messages(topic, key, value, on_delivery):
        existing_message = next((m for m in sent_messages if m['key'] == key), None)
        assert existing_message is None, f"Duplicate message with key {key} attempted to be produced"

        sent_messages.append({
            'topic': topic,
            'key': key,
            'value': value
        })

    mock_kafka_producer.producer.produce.side_effect = capture_messages

    for url in sample_image_urls['images']:
        test_message = InputImageMessage(
            job_id=f"unique-job-{url.split('/')[-1]}",
            image_url=url
        )
        await mock_kafka_producer.send_message(test_message)

    assert len(sent_messages) == len(sample_image_urls['images'])

def test_topic_creation_failure(mocker):
    """Test handling of topic creation failures"""
    mock_admin_client_class = mocker.patch('dish_images_processor.kafka.topics.AdminClient')
    mock_admin_client_instance = mocker.MagicMock()
    mock_admin_client_class.return_value = mock_admin_client_instance

    mock_admin_client_instance.create_topics.return_value = {
        "test-topic": mocker.MagicMock(
            result=mocker.MagicMock(side_effect=Exception("Creation failed"))
        )
    }

    topic_manager = TopicManager()
    topic_manager.create_topics(["test-topic"])

    mock_admin_client_instance.create_topics.assert_called_once()

def test_topic_exists_handling(mocker):
    """Test proper handling of already existing topics"""
    mock_admin_client_class = mocker.patch('dish_images_processor.kafka.topics.AdminClient')
    mock_admin_client_instance = mocker.MagicMock()
    mock_admin_client_class.return_value = mock_admin_client_instance

    mocker.patch.object(
        TopicManager,
        'get_existing_topics',
        return_value={"existing-topic"}
    )

    topic_manager = TopicManager()
    topic_manager.create_topics(["existing-topic", "new-topic"])

    create_topics_call = mock_admin_client_instance.create_topics.call_args
    # Should only try to create new-topic since existing-topic already exists
    assert len(create_topics_call[0][0]) == 1
    assert create_topics_call[0][0][0].topic == "new-topic"

@pytest.mark.asyncio
async def test_producer_delivery_failure(mock_kafka_producer, mocker):
    """Test handling of message delivery failures"""
    test_message = InputImageMessage(
        job_id="test-123",
        image_url="http://example.com/test.jpg"
    )

    mock_producer = mocker.MagicMock()
    mock_producer.produce.side_effect = Exception("Delivery failed")
    mock_kafka_producer.producer = mock_producer

    with pytest.raises(Exception):
        await mock_kafka_producer.send_message(test_message)

@pytest.mark.asyncio
async def test_producer_flush_timeout(mock_kafka_producer, mocker):
    """Test handling of flush timeouts"""
    test_message = InputImageMessage(
        job_id="test-123",
        image_url="http://example.com/test.jpg"
    )

    mock_producer = mocker.MagicMock()
    mock_producer.flush.side_effect = Exception("Flush timeout")
    mock_kafka_producer.producer = mock_producer

    with pytest.raises(Exception):
        await mock_kafka_producer.send_message(test_message)

@pytest.mark.asyncio
async def test_consumer_rebalance_callback(mocker):
    """Test consumer behavior during rebalancing"""
    process_message = AsyncMock()
    consumer = MessageConsumer(
        topic="test-topic",
        process_message=process_message,
        group_id="test-group",
        limiter=mocker.MagicMock()
    )

    # Simulate rebalance by triggering error
    message = mocker.MagicMock()
    message.error.return_value = mocker.MagicMock(
        code=mocker.MagicMock(return_value=KafkaError._PARTITION_EOF)
    )

    consumer.consumer = mocker.MagicMock()
    consumer.consumer.poll.return_value = message

    result = consumer._poll_message()
    assert result is None
    assert not process_message.called

@pytest.mark.asyncio
async def test_consumer_message_during_rebalance(mocker):
    """Test message handling during rebalance"""
    process_message = AsyncMock()
    consumer = MessageConsumer(
        topic="test-topic",
        process_message=process_message,
        group_id="test-group",
        limiter=mocker.MagicMock()
    )

    messages = [
        mocker.MagicMock(
            error=mocker.MagicMock(return_value=None),
            value=mocker.MagicMock(return_value=json.dumps({
                "job_id": "test-123",
                "image_url": "http://example.com/test.jpg"
            }).encode('utf-8'))
        ),
        mocker.MagicMock(
            error=mocker.MagicMock(return_value=mocker.MagicMock(
                code=mocker.MagicMock(return_value=KafkaError._PARTITION_EOF)
            ))
        )
    ]

    consumer.consumer = mocker.MagicMock()
    consumer.consumer.poll.side_effect = messages

    # First message should be processed
    result1 = consumer._poll_message()
    assert result1 is not None

    # Second message (during rebalance) should be skipped
    result2 = consumer._poll_message()
    assert result2 is None

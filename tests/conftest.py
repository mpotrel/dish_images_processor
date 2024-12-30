import pytest
from datetime import datetime
from unittest.mock import AsyncMock
from fastapi.testclient import TestClient

from dish_images_processor.app import app
from dish_images_processor.kafka.producer import MessageProducer
from dish_images_processor.services.base_service import ImageProcessingService
from dish_images_processor.utils.concurrency import ConcurrencyLimiter

@pytest.fixture(autouse=True)
def mock_kafka_dependencies(mocker):
    """Mock all Kafka-related dependencies"""
    mock_consumer = mocker.MagicMock()
    mock_producer = mocker.MagicMock()

    mocker.patch('confluent_kafka.Consumer', return_value=mock_consumer)
    mocker.patch('confluent_kafka.Producer', return_value=mock_producer)
    mocker.patch('confluent_kafka.admin.AdminClient', return_value=mocker.MagicMock())

@pytest.fixture
def test_client():
    return TestClient(app)

@pytest.fixture
def sample_image_urls():
    return {
        "images": [
            "http://example.com/image1.jpg",
            "http://example.com/image2.jpg"
        ]
    }

@pytest.fixture
def sample_message():
    return {
        "job_id": "test-123",
        "image_url": "http://example.com/test.jpg",
        "created_at": datetime.utcnow().isoformat()
    }

@pytest.fixture
def mock_kafka_producer(mocker):
    producer = MessageProducer("test-service", "test-topic")
    mock_producer = mocker.MagicMock()
    mock_producer.flush.return_value = 0
    producer.producer = mock_producer
    return producer

@pytest.fixture
def mock_kafka_consumer(mocker):
    consumer = mocker.MagicMock()
    consumer.subscribe = mocker.MagicMock()
    consumer.poll.return_value = None
    consumer.close = mocker.MagicMock()
    consumer.commit = mocker.MagicMock()
    return consumer

@pytest.fixture
def mock_producers(mocker):
    """Mock the producers dependency"""
    producers = {
        'background_removal': mocker.MagicMock(),
        'background_generation': mocker.MagicMock(),
        'hyper_resolution': mocker.MagicMock(),
        'completed_images': mocker.MagicMock()
    }
    for producer in producers.values():
        producer.send_message = AsyncMock()

    mocker.patch('dish_images_processor.dependencies.producers', producers)
    return producers

@pytest.fixture
def mock_service():
    return ImageProcessingService("background_removal")

@pytest.fixture
def mock_limiter():
    return ConcurrencyLimiter(2)

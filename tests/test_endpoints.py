import pytest
from fastapi import HTTPException

def test_preprocess_endpoint_success(test_client, mock_producers, sample_image_urls):
    response = test_client.post("/v0/preprocess", json=sample_image_urls)

    assert response.status_code == 200
    response_data = response.json()
    assert "job_id" in response_data
    assert response_data["status"] == "processing"

    assert mock_producers['background_removal'].send_message.call_count == len(sample_image_urls["images"])

def test_preprocess_endpoint_failure(test_client, mock_producers, sample_image_urls):
    mock_producers['background_removal'].send_message.side_effect = Exception("Failed to send message")

    response = test_client.post("/v0/preprocess", json=sample_image_urls)

    assert response.status_code == 500
    assert "Failed to send message" in response.json()["detail"]

def test_preprocess_endpoint_invalid_input(test_client, mock_producers):
    response = test_client.post("/v0/preprocess", json={"images": ["not-a-url"]})
    assert response.status_code == 422

    response = test_client.post("/v0/preprocess", json={"wrong_field": []})
    assert response.status_code == 422

    response = test_client.post("/v0/preprocess", json={})
    assert response.status_code == 422

def test_preprocess_endpoint_empty_images(test_client, mock_producers):
    response = test_client.post("/v0/preprocess", json={"images": []})
    assert response.status_code == 422

def test_missing_producer(test_client, mocker):
    mocker.patch('dish_images_processor.dependencies.producers', {})
    response = test_client.post("/v0/preprocess",
                              json={"images": ["http://example.com/test.jpg"]})
    assert response.status_code == 500

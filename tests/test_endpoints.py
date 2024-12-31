from dish_images_processor.models.messages import OutputImageMessage


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

def test_get_image_pairs(test_client, mock_producers, mocker):
    """Test getting image pairs for a job"""
    test_messages = {
        "test_job": [
            OutputImageMessage(
                job_id="test_job-0",
                image_url="http://example.com/1",
                unprocessed_image_url="http://example.com/1",
                processed_image_url="http://example.com/processed1",
                created_at="2024-01-01T00:00:00"
            ),
            OutputImageMessage(
                job_id="test_job-1",
                image_url="http://example.com/2",
                unprocessed_image_url="http://example.com/2",
                processed_image_url="http://example.com/processed2",
                created_at="2024-01-01T00:00:00"
            )
        ]
    }

    # Override the dependency (hacky but mocking failed)
    from dish_images_processor.app import app
    from dish_images_processor.dependencies import get_completed_messages

    def override_get_messages():
        return test_messages

    app.dependency_overrides[get_completed_messages] = override_get_messages

    try:
        response = test_client.get("/v0/images/test_job")
        assert response.status_code == 200
    finally:
        app.dependency_overrides.clear()

def test_get_image_pairs_not_found(test_client, mocker):
    """Test getting image pairs for a nonexistent job"""
    mocker.patch('dish_images_processor.dependencies.get_completed_messages', return_value={})

    response = test_client.get("/v0/images/nonexistent-job")
    assert response.status_code == 404

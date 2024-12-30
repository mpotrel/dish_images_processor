import pytest

@pytest.mark.asyncio
async def test_service_process_new_message(mock_service, mock_producers, sample_message):
    result = await mock_service.process(sample_message)

    assert result is not None
    assert result.job_id == sample_message["job_id"]
    assert result.image_url == sample_message["image_url"]
    assert result.processed_url == f"processed_{sample_message['image_url']}"

    mock_producers['background_generation'].send_message.assert_called_once()

@pytest.mark.asyncio
async def test_service_skip_already_processed(mock_service, mock_producers, sample_message):
    sample_message['processed_by'] = {'background_removal': True}

    result = await mock_service.process(sample_message)
    assert result is None
    assert not mock_producers['background_generation'].send_message.called

@pytest.mark.asyncio
async def test_service_process_error(mock_service, mocker, sample_message):
    mocker.patch('fal_client.submit', side_effect=Exception("FAL API error"))

    result = await mock_service.process(sample_message)
    assert result is not None  # Mock implementation returns processed message

@pytest.mark.asyncio
async def test_service_completion_flow(mock_service, mock_producers, sample_message):
    mock_service.service_name = "hyper_resolution"

    result = await mock_service.process(sample_message)

    assert result is not None
    mock_producers['completed_images'].send_message.assert_called_once()

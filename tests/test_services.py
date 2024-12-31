import pytest

@pytest.mark.asyncio
async def test_service_process_new_message(mock_service, mock_producers, sample_message):
    result = await mock_service.process(sample_message)

    assert result is not None
    assert result.job_id == sample_message["job_id"]
    assert result.image_url == sample_message["image_url"]
    assert result.processed_image_url == f"processed_{sample_message['image_url']}"

    mock_producers['background_generation'].send_message.assert_called_once()

@pytest.mark.asyncio
async def test_service_skip_already_processed(mock_service, mock_producers, sample_message):
    sample_message['processed_by'] = {'background_removal': True}

    result = await mock_service.process(sample_message)
    assert result is None
    assert not mock_producers['background_generation'].send_message.called

@pytest.mark.asyncio
async def test_service_process_error(mock_service, mocker, sample_message):
    mock_service.request_fal.side_effect = Exception("FAL API error")
    result = await mock_service.process(sample_message)
    assert result is None

@pytest.mark.asyncio
async def test_service_completion_flow(mock_service, mock_producers, sample_message):
    mock_service.service_name = "hyper_resolution"

    result = await mock_service.process(sample_message)

    assert result is not None
    mock_producers['completed_images'].send_message.assert_called_once()
    # tests/test_services.py

@pytest.mark.asyncio
async def test_message_forwarding_to_next_service(mock_service, mock_producers, sample_message):
    """Test message is forwarded to the next service in the pipeline"""
    mock_service.service_name = "background_removal"

    result = await mock_service.process(sample_message)

    assert result is not None
    # Should forward to background_generation
    mock_producers['background_generation'].send_message.assert_called_once()
    # Should not forward to completed_images
    assert not mock_producers['completed_images'].send_message.called

@pytest.mark.asyncio
async def test_message_forwarding_last_service(mock_service, mock_producers, sample_message):
    """Test message is forwarded to completed_images when at end of pipeline"""
    mock_service.service_name = "hyper_resolution"

    result = await mock_service.process(sample_message)

    assert result is not None
    # Should forward to completed_images
    mock_producers['completed_images'].send_message.assert_called_once()
    # Should not forward to other services
    assert not mock_producers['background_generation'].send_message.called

@pytest.mark.asyncio
async def test_message_forwarding_error_handling(mock_service, mock_producers, sample_message):
    """Test error handling when forwarding fails"""
    mock_service.service_name = "background_removal"
    mock_producers['background_generation'].send_message.side_effect = Exception("Forwarding failed")

    result = await mock_service.process(sample_message)

    assert result is not None  # Process should complete despite forwarding error

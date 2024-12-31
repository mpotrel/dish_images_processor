from pydantic import ValidationError
import pytest

from dish_images_processor.config.settings import AppSettings, get_app_settings, get_settings

def test_invalid_service_settings():
    """Test handling of invalid service configuration"""
    with pytest.raises(ValueError):
        get_settings("nonexistent_service")

def test_missing_required_settings(mocker):
    """Test handling of missing required settings"""
    mock_yaml = mocker.patch('yaml.safe_load')
    mock_yaml.return_value = {
        "test_service": {
            # Missing required 'endpoint' field
            "arguments": {}
        }
    }

    with pytest.raises(ValueError):
        get_settings("test_service")

def test_valid_service_settings(mocker):
    """Test valid service configuration"""
    test_config = {
        "background_removal": {
            "endpoint": "test-endpoint",
            "arguments": {"arg1": "value1"}
        }
    }

    mocker.patch('dish_images_processor.config.settings.yaml.safe_load', return_value=test_config)
    mocker.patch('dish_images_processor.config.settings.get_services_settings', return_value=test_config)

    settings = get_settings("background_removal")
    assert settings.endpoint == "test-endpoint"
    assert settings.arguments == {"arg1": "value1"}

def test_environment_variable_validation(mocker):
    """Test validation of required environment variables"""
    get_app_settings.cache_clear()

    mocker.patch.dict('os.environ', {
        'FAL_KEY': '',
        'KAFKA_BOOTSTRAP_SERVERS': '',
        'MAX_CONCURRENT_REQUESTS': '0'
    }, clear=True)

    with pytest.raises(ValidationError):
        AppSettings()

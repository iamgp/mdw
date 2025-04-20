"""Tests for the Nightscout extractor workflow."""

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

# Keep logger import if needed elsewhere, but remove the test-specific config
# from loguru import logger
from data_warehouse.workflows.core.exceptions import ExtractorError, ValidationError
from data_warehouse.workflows.nightscout.extraction import GlucoseData, NightscoutExtractor

# Removed global Loguru configuration


# ... fixtures ...
@pytest.fixture
def valid_config() -> dict:
    """Provides a valid configuration dictionary for NightscoutExtractor."""
    return {
        "base_url": "https://fake-nightscout.com",
        "api_secret": "fake_secret",
        "lookback_hours": 1,
        # Base sling_config can be empty or have defaults
        "sling_config": {"timeout": 30},
    }


@pytest.fixture
def mock_sling_stream_success(mocker: MagicMock):
    """Mocks sling.Sling().stream() to return sample Nightscout data."""
    mock_data = [
        {"_id": "1", "sgv": 120, "dateString": "2023-10-27T10:00:00.000Z", "device": "DexcomG6"},
        {"_id": "2", "sgv": 125, "dateString": "2023-10-27T10:05:00.000Z", "device": "DexcomG6"},
        {"_id": "3", "sgv": 118, "dateString": "2023-10-27T10:10:00.000Z", "device": "DexcomG6"},
    ]
    mock_sling_instance = MagicMock()
    mock_sling_instance.stream.return_value = mock_data
    return mocker.patch("sling.Sling", return_value=mock_sling_instance)


@pytest.fixture
def mock_sling_stream_transform_error(mocker: MagicMock):
    """Mocks sling.Sling().stream() to return data with transformation errors."""
    mock_data = [
        {"_id": "1", "sgv": 120, "dateString": "2023-10-27T10:00:00.000Z"},  # Valid
        {"_id": "2", "sgv": None, "dateString": "2023-10-27T10:05:00.000Z"},  # Invalid sgv (None)
        {"_id": "3", "sgv": 118},  # Invalid - missing dateString
        {"_id": "4", "sgv": "invalid-float", "dateString": "2023-10-27T10:15:00.000Z"},  # Invalid sgv (str)
        {"_id": "5", "sgv": 130, "dateString": "not-a-date"},  # Invalid dateString format
        {"_id": "6", "sgv": 140, "dateString": "2023-10-27T10:20:00.000Z"},  # Valid
    ]
    mock_sling_instance = MagicMock()
    mock_sling_instance.stream.return_value = mock_data
    return mocker.patch("sling.Sling", return_value=mock_sling_instance)


@pytest.fixture
def mock_sling_stream_api_error(mocker: MagicMock):
    """Mocks sling.Sling().stream() to raise an Exception simulating an API error."""
    mock_sling_instance = MagicMock()
    # Raise a standard Exception, as Sling might not expose specific error types
    # The extractor logic should catch this and wrap it.
    mock_sling_instance.stream.side_effect = Exception("Simulated Sling API request failed")
    return mocker.patch("sling.Sling", return_value=mock_sling_instance)


# --- Test Cases ---


# ... other tests ...
def test_nightscout_extractor_init_success(valid_config: dict):
    """Test successful initialization of NightscoutExtractor."""
    try:
        extractor = NightscoutExtractor(name="test_ns", config=valid_config)
        assert extractor.name == "test_ns"
        # Assertions should check the final state of self._sling_config after merging
        # The base class stores the relevant part in self._sling_config
        assert extractor._sling_config.get("source") == "api"
        assert "url" in extractor._sling_config.get("source_options", {})
        assert extractor._sling_config["source_options"]["url"].startswith("https://fake-nightscout.com")
        assert extractor._sling_config["source_options"]["auth"]["token"] == "fake_secret"
        # Check that the base config value was merged in
        assert extractor._sling_config.get("timeout") == 30
        # Also check the Nightscout-specific config is stored
        assert extractor.nightscout_config.base_url == "https://fake-nightscout.com"

    except Exception as e:
        pytest.fail(f"Initialization failed unexpectedly: {e}")


def test_nightscout_extractor_init_missing_config():
    """Test initialization failure with missing required config keys."""
    with pytest.raises(ValidationError, match="requires 'base_url' and 'api_secret'"):
        NightscoutExtractor(name="test_ns_fail", config={"lookback_hours": 1})
    with pytest.raises(ValidationError, match="requires 'base_url' and 'api_secret'"):
        NightscoutExtractor(name="test_ns_fail", config={"base_url": "url"})
    with pytest.raises(ValidationError, match="requires 'base_url' and 'api_secret'"):
        NightscoutExtractor(name="test_ns_fail", config={"api_secret": "secret"})
    with pytest.raises(ValidationError, match="requires 'base_url' and 'api_secret'"):
        NightscoutExtractor(name="test_ns_fail", config={})


def test_nightscout_extract_success(valid_config: dict, mock_sling_stream_success: MagicMock):
    """Test successful data extraction and transformation."""
    extractor = NightscoutExtractor(name="test_ns_success", config=valid_config)
    result = extractor.extract()

    assert isinstance(result, GlucoseData)
    assert len(result.readings) == 3
    assert result.readings[0].glucose == 120.0
    assert result.readings[1].glucose == 125.0
    # Make comparison timezone-aware (dateutil.parser adds tzinfo)
    expected_dt = datetime(2023, 10, 27, 10, 10, 0, tzinfo=UTC)
    assert result.readings[2].timestamp == expected_dt
    assert result.device_info["extractor"] == "test_ns_success"
    mock_sling_stream_success.assert_called_once()  # Check Sling was initialized
    mock_sling_stream_success.return_value.stream.assert_called_once()  # Check stream was called


def test_nightscout_extract_handles_transform_errors(
    valid_config: dict, mock_sling_stream_transform_error: MagicMock, mocker: MagicMock
):
    """Test that extraction skips records with transformation errors."""
    extractor = NightscoutExtractor(name="test_ns_transform_err", config=valid_config)

    # Patch the .warning method *on the specific logger instance* used by the extractor
    mock_logger_warning = mocker.patch.object(extractor.logger, "warning", autospec=True)

    result = extractor.extract()

    assert isinstance(result, GlucoseData)
    # Only 2 out of 6 records were valid
    assert len(result.readings) == 2
    assert result.readings[0].glucose == 120.0
    assert result.readings[1].glucose == 140.0

    # Check that the mocked logger.warning was called for the 4 invalid records
    assert mock_logger_warning.call_count == 4

    # Check the content of the warning calls (can check specific messages if needed)
    call_args_list = mock_logger_warning.call_args_list
    # Example check for the first warning message content
    first_call_args, _ = call_args_list[0]
    assert "Skipping record due to transformation/validation error" in first_call_args[0]
    assert "Missing 'sgv' value" in first_call_args[0]

    # Example check for another specific error message
    fourth_call_args, _ = call_args_list[3]
    assert "Skipping record due to transformation/validation error" in fourth_call_args[0]
    # Note: dateutil parser error might be slightly different depending on version
    assert "Unknown string format: not-a-date" in fourth_call_args[0]

    mock_sling_stream_transform_error.assert_called_once()
    mock_sling_stream_transform_error.return_value.stream.assert_called_once()


# ... rest of tests ...
def test_nightscout_extract_sling_api_error(valid_config: dict, mock_sling_stream_api_error: MagicMock):
    """Test that Sling API errors are wrapped in ExtractorError."""
    extractor = NightscoutExtractor(name="test_ns_api_err", config=valid_config)

    # Expect the generic Exception to be caught and wrapped
    with pytest.raises(
        ExtractorError,
        match="Unexpected error during Nightscout extraction/transformation: Simulated Sling API request failed",
    ):
        extractor.extract()

    mock_sling_stream_api_error.assert_called_once()
    mock_sling_stream_api_error.return_value.stream.assert_called_once()


def test_nightscout_extract_unexpected_error(valid_config: dict, mocker: MagicMock):
    """Test that unexpected errors during extraction are wrapped."""
    # Mock sling.Sling to raise a generic exception after initialization
    mock_sling_instance = MagicMock()
    mock_sling_instance.stream.side_effect = ValueError("Something unexpected")
    mocker.patch("sling.Sling", return_value=mock_sling_instance)

    extractor = NightscoutExtractor(name="test_ns_unexpected_err", config=valid_config)

    with pytest.raises(
        ExtractorError, match="Unexpected error during Nightscout extraction/transformation: Something unexpected"
    ):
        extractor.extract()

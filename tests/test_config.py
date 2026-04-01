"""Tests for configuration management."""
from pathlib import Path

from claw_data_filter.config import Config


def test_config_defaults():
    """Test config default values."""
    config = Config()
    assert config.llm_endpoint == "http://localhost:8000/v1"
    assert config.llm_api_key is None
    assert config.db_path == Path("./data.duckdb")
    assert config.worker_count > 0
    assert config.batch_size == 10
    assert config.max_retries == 3


def test_config_custom_values():
    """Test config with custom values."""
    config = Config(
        llm_endpoint="http://custom:8000/v1",
        llm_api_key="test-key",
        db_path=Path("/tmp/test.duckdb"),
        worker_count=4,
        batch_size=20,
        max_retries=5,
    )
    assert config.llm_endpoint == "http://custom:8000/v1"
    assert config.llm_api_key == "test-key"
    assert config.db_path == Path("/tmp/test.duckdb")
    assert config.worker_count == 4
    assert config.batch_size == 20
    assert config.max_retries == 5


def test_config_from_env():
    """Test config from environment variables."""
    import os
    os.environ["LLM_ENDPOINT"] = "http://custom:8000/v1"
    os.environ["LLM_API_KEY"] = "test-key"
    os.environ["DB_PATH"] = "/tmp/test.duckdb"
    os.environ["WORKER_COUNT"] = "4"
    os.environ["BATCH_SIZE"] = "20"
    os.environ["MAX_RETRIES"] = "5"

    config = Config.from_env()
    assert config.llm_endpoint == "http://custom:8000/v1"
    assert config.llm_api_key == "test-key"
    assert config.db_path == Path("/tmp/test.duckdb")
    assert config.worker_count == 4
    assert config.batch_size == 20
    assert config.max_retries == 5

    # Cleanup
    del os.environ["LLM_ENDPOINT"]
    del os.environ["LLM_API_KEY"]
    del os.environ["DB_PATH"]
    del os.environ["WORKER_COUNT"]
    del os.environ["BATCH_SIZE"]
    del os.environ["MAX_RETRIES"]


def test_config_round_feedback_defaults():
    """Test round feedback config defaults"""
    from claw_data_filter.config import Config

    config = Config()
    assert config.max_concurrency == 10
    assert config.llm_timeout == 60.0
    assert config.context_window == 4096


def test_config_from_env_round_feedback():
    """Test round feedback config from env"""
    import os
    os.environ["MAX_CONCURRENCY"] = "20"
    os.environ["LLM_TIMEOUT"] = "30.0"
    os.environ["CONTEXT_WINDOW"] = "8192"

    config = Config.from_env()
    assert config.max_concurrency == 20
    assert config.llm_timeout == 30.0
    assert config.context_window == 8192

    # Cleanup
    del os.environ["MAX_CONCURRENCY"]
    del os.environ["LLM_TIMEOUT"]
    del os.environ["CONTEXT_WINDOW"]
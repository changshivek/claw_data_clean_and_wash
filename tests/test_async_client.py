import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from claw_data_filter.llm.async_client import AsyncLLMClient

@pytest.mark.asyncio
async def test_async_client_initialization():
    """Test AsyncLLMClient can be initialized"""
    client = AsyncLLMClient(
        endpoint="http://localhost:8000/v1",
        timeout=60.0,
    )
    assert client.endpoint == "http://localhost:8000/v1"
    assert client.timeout == 60.0
    await client.close()

@pytest.mark.asyncio
async def test_chat_returns_content():
    """Test chat method returns string content"""
    client = AsyncLLMClient(endpoint="http://localhost:8000/v1")

    # Mock the httpx response - json() is synchronous in httpx
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = {
        "choices": [{"message": {"content": "need_tool=yes; tool_correct=no"}}]
    }

    with patch.object(client.client, 'post', return_value=mock_response):
        result = await client.chat([{"role": "user", "content": "test"}])
        assert result == "need_tool=yes; tool_correct=no"

    await client.close()
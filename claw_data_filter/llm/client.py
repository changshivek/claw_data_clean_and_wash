"""Local LLM API client (vLLM/Ollama compatible)."""
import logging
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class LLMClient:
    """HTTP client for local LLM inference servers (vLLM/Ollama).

    Communicates via the OpenAI-compatible /chat/completions endpoint.
    """

    def __init__(
        self,
        endpoint: str = "http://localhost:8000/v1",
        api_key: str | None = None,
        model: str | None = None,
        max_retries: int = 3,
    ):
        """Initialize LLM client.

        Args:
            endpoint: Base URL of the LLM API server
            api_key: Optional API key for authentication
            model: Optional model name (sent to server)
            max_retries: Maximum number of retry attempts on failure
        """
        self.endpoint = endpoint.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.max_retries = max_retries
        self.timeout = 120.0  # 2 minutes for evaluation

        headers = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        self.client = httpx.Client(timeout=self.timeout, headers=headers)

    def chat(self, messages: list[dict[str, str]], *, temperature: float = 0.1) -> str:
        """Send chat request to LLM, return assistant message content.

        Args:
            messages: List of message dicts with 'role' and 'content' keys
            temperature: Sampling temperature (lower = more deterministic)

        Returns:
            Content of the assistant's response message

        Raises:
            httpx.HTTPStatusError: On HTTP errors after all retries
            RuntimeError: On unexpected errors after all retries
        """
        payload: dict[str, Any] = {
            "messages": messages,
            "temperature": temperature,
        }
        if self.model:
            payload["model"] = self.model

        for attempt in range(self.max_retries):
            try:
                response = self.client.post(
                    f"{self.endpoint}/chat/completions",
                    json=payload,
                )
                response.raise_for_status()
                data = response.json()
                return data["choices"][0]["message"]["content"]
            except httpx.HTTPStatusError as e:
                logger.warning(f"Attempt {attempt + 1}/{self.max_retries} failed: HTTP {e.response.status_code}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)  # exponential backoff
                else:
                    raise
            except (httpx.ConnectError, httpx.TimeoutException) as e:
                logger.warning(f"Attempt {attempt + 1}/{self.max_retries} failed: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise

        raise RuntimeError("Should not reach here")

    def close(self):
        """Close HTTP client."""
        self.client.close()
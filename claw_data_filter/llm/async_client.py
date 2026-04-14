"""Async LLM API client for high-concurrency round judgment calls."""
import logging
import re
from typing import Any

import httpx

logger = logging.getLogger(__name__)


def _compact_text(value: str, limit: int = 800) -> str:
    compact = re.sub(r"\s+", " ", value or "").strip()
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3] + "..."


class LLMRequestError(RuntimeError):
    """Structured request failure surfaced to round-feedback retry logic."""


class AsyncLLMClient:
    """Async HTTP client for local LLM inference servers (vLLM/Ollama).

    Communicates via the OpenAI-compatible /chat/completions endpoint.
    Designed for high-concurrency scenarios with semaphore control.
    """

    def __init__(
        self,
        endpoint: str = "http://localhost:8000/v1",
        api_key: str | None = None,
        model: str | None = None,
        timeout: float = 60.0,
    ):
        """Initialize async LLM client.

        Args:
            endpoint: Base URL of the LLM API server
            api_key: Optional API key for authentication
            model: Optional model name (sent to server)
            timeout: Request timeout in seconds
        """
        self.endpoint = endpoint.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.timeout = timeout

        headers = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=min(10.0, timeout), read=timeout, write=timeout, pool=timeout),
            headers=headers,
        )

    async def chat(
        self,
        messages: list[dict[str, str]],
        *,
        temperature: float = 0.1,
        max_tokens: int = 1024,
    ) -> str:
        """Send chat request to LLM, return assistant message content.

        Args:
            messages: List of message dicts with 'role' and 'content' keys
            temperature: Sampling temperature (lower = more deterministic)
            max_tokens: Maximum tokens to generate

        Returns:
            Content of the assistant's response message

        Raises:
            httpx.HTTPStatusError: On HTTP errors
            httpx.TimeoutException: On timeout
        """
        payload: dict[str, Any] = {
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if self.model:
            payload["model"] = self.model

        # Disable thinking mode for Qwen models
        # chat_template_kwargs must be at top level, not in extra_body
        payload["chat_template_kwargs"] = {"enable_thinking": False}

        try:
            response = await self.client.post(
                f"{self.endpoint}/chat/completions",
                json=payload,
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            response_text = _compact_text(exc.response.text)
            raise LLMRequestError(
                f"HTTP {exc.response.status_code} from {exc.request.url}; body={response_text or '<empty>'}"
            ) from exc
        except httpx.TimeoutException as exc:
            raise LLMRequestError(
                f"{exc.__class__.__name__} after {self.timeout:.1f}s for {self.endpoint}/chat/completions"
            ) from exc
        except httpx.RequestError as exc:
            raise LLMRequestError(
                f"{exc.__class__.__name__} contacting {self.endpoint}/chat/completions: {exc!r}"
            ) from exc

        data = response.json()
        content = data["choices"][0]["message"].get("content")
        if content is None:
            raise ValueError("LLM returned empty content")
        return content

    async def close(self):
        """Close async HTTP client."""
        await self.client.aclose()
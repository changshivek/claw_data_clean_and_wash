"""Sample model for raw conversation data."""
from typing import Optional, Any
from datetime import datetime
from pydantic import BaseModel, Field
import json


class Sample(BaseModel):
    """Represents a single agent conversation sample."""

    id: Optional[int] = None
    raw_json: dict[str, Any] = Field(default_factory=dict)
    user_query: str = ""
    assistant_response: str = ""
    num_turns: int = 0
    num_tool_calls: int = 0
    has_error: bool = False
    imported_at: datetime = Field(default_factory=datetime.now)

    @classmethod
    def from_dict(cls, data: dict) -> "Sample":
        """Parse from OpenAI format dict.

        Input format:
        {
            "messages": [
                {"role": "system", "content": "..."},
                {"role": "user", "content": "..."},
                {"role": "assistant", "content": "...", "tool_calls": [...]},
                {"role": "tool", "content": "...", "tool_call_id": "..."},
                ...
            ]
        }
        """
        messages = data.get("messages", [])

        # Extract user query (last user message)
        user_query = ""
        for msg in reversed(messages):
            if msg.get("role") == "user":
                user_query = msg.get("content", "")
                break

        # Extract formatted assistant response (concatenate all assistant messages)
        assistant_parts = []
        tool_calls = []
        for msg in messages:
            if msg.get("role") == "assistant":
                content = msg.get("content", "")
                if content:
                    assistant_parts.append(content)
                tc = msg.get("tool_calls", [])
                tool_calls.extend(tc)

        assistant_response = "\n".join(assistant_parts)

        # Count turns (user-assistant pairs = number of user messages)
        num_turns = sum(1 for msg in messages if msg.get("role") == "user")

        # Count tool calls
        num_tool_calls = len(tool_calls)

        # Check for errors (tool results that indicate errors)
        has_error = False
        for msg in messages:
            if msg.get("role") == "tool":
                content = msg.get("content", "")
                if content and ("error" in content.lower() or "exception" in content.lower()):
                    has_error = True
                    break

        return cls(
            raw_json=data,
            user_query=user_query,
            assistant_response=assistant_response,
            num_turns=num_turns,
            num_tool_calls=num_tool_calls,
            has_error=has_error,
        )
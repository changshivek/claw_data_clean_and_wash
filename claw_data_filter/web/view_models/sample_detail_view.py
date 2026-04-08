"""Detail-page view models."""
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class TurnDetailView:
    turn_index: int
    user_message: str
    assistant_message: str
    tool_calls: list[dict[str, Any]]
    tool_result: str | None
    response_helpful: str | None
    user_satisfied: str | None
    signal_from_users: list[str] = field(default_factory=list)
    llm_error: bool = False


@dataclass(slots=True)
class SampleDetailView:
    sample_id: int
    sample_uid: str
    num_turns: int
    expected_judgment_count: int
    num_tool_calls: int
    helpful_rate: float
    satisfied_rate: float
    processing_status: str
    session_merge_status: str | None = None
    session_merge_keep: bool | None = None
    session_merge_group_id: str | None = None
    session_merge_group_size: int | None = None
    session_merge_representative_id: int | None = None
    session_merge_reason: str | None = None
    turns: list[TurnDetailView] = field(default_factory=list)

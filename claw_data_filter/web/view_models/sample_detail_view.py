"""Detail-page view models."""
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ResponseStepDetailView:
    response_index: int
    episode_index: int
    assistant_message_index: int
    user_message: str
    assistant_message: str
    tool_calls: list[dict[str, Any]]
    feedback_kind: str
    feedback_message_start_index: int | None
    feedback_message_end_index: int | None
    feedback_payload: list[str] = field(default_factory=list)
    response_progress: str | None = None
    llm_error: bool = False


@dataclass(slots=True)
class EpisodeDetailView:
    episode_index: int
    start_user_message_index: int
    end_before_user_message_index: int | None
    user_message: str
    assistant_messages: list[str] = field(default_factory=list)
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    tool_results: list[str] = field(default_factory=list)
    signal_from_users: list[str] = field(default_factory=list)
    user_satisfied: str | None = None
    llm_error: bool = False


@dataclass(slots=True)
class SampleDetailView:
    sample_id: int
    sample_uid: str
    empty_response: bool
    num_turns: int
    expected_judgment_count: int
    expected_response_judgment_count: int
    expected_episode_judgment_count: int
    num_tool_calls: int
    progress_rate: float
    regress_rate: float
    satisfied_rate: float
    processing_status: str
    session_merge_status: str | None = None
    session_merge_keep: bool | None = None
    session_merge_group_id: str | None = None
    session_merge_group_size: int | None = None
    session_merge_representative_uid: str | None = None
    session_merge_reason: str | None = None
    response_steps: list[ResponseStepDetailView] = field(default_factory=list)
    user_episodes: list[EpisodeDetailView] = field(default_factory=list)

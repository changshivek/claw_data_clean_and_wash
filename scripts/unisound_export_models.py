"""Pydantic models for OpenAI round feedback input and Unisound output."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class OpenAIConversationMessage(BaseModel):
    """Normalized OpenAI-style message used by exported records."""

    model_config = ConfigDict(extra="allow")

    role: Literal["system", "developer", "user", "assistant", "tool"]
    content: Any = None
    tool_calls: list[dict[str, Any]] = Field(default_factory=list)
    tool_call_id: str | None = None


class OpenAIConversation(BaseModel):
    """Conversation payload in exported OpenAI round feedback records."""

    model_config = ConfigDict(extra="allow")

    messages: list[OpenAIConversationMessage]
    tools: list[dict[str, Any]] = Field(default_factory=list)


class ResponseProgressStep(BaseModel):
    """Assistant response-level round feedback."""

    model_config = ConfigDict(extra="allow")

    response_index: int
    episode_index: int
    assistant_message_index: int
    feedback_kind: str
    feedback_message_start_index: int | None = None
    feedback_message_end_index: int | None = None
    feedback_payload: list[Any] = Field(default_factory=list)
    response_progress: str | None = None
    llm_error: bool = False


class UserSatisfiedEpisode(BaseModel):
    """User episode-level round feedback."""

    model_config = ConfigDict(extra="allow")

    episode_index: int
    message_start_index: int
    message_end_index: int
    signal_from_users: list[Any] = Field(default_factory=list)
    user_satisfied: str | None = None
    llm_error: bool = False


class OpenAIRoundFeedbackBlock(BaseModel):
    """Round feedback sidecar structure."""

    model_config = ConfigDict(extra="allow")

    response_progress_steps: list[ResponseProgressStep] = Field(default_factory=list)
    user_satisfied_episodes: list[UserSatisfiedEpisode] = Field(default_factory=list)


class OpenAIRoundFeedbackMetadata(BaseModel):
    """Sample-level metadata in exported records."""

    model_config = ConfigDict(extra="allow")

    sample_uid: str
    local_sample_id: int | None = None
    imported_at: str | None = None


class OpenAISourceMetadata(BaseModel):
    """Source metadata sidecar."""

    model_config = ConfigDict(extra="allow")

    timestamp: str | None = None
    model_requested: str | None = None
    user_agent: str | None = None
    request_id: str | None = None
    trace_id: str | None = None
    source_format: str | None = None
    metadata: Any = None


class OpenAIRoundFeedbackRecord(BaseModel):
    """Validated input record for conversion."""

    model_config = ConfigDict(extra="allow")

    record_schema: Literal["openai_round_feedback_v2"] = Field(alias="schema")
    metadata: OpenAIRoundFeedbackMetadata
    source_metadata: OpenAISourceMetadata
    conversation: OpenAIConversation
    round_feedback: OpenAIRoundFeedbackBlock


class UnisoundAssistant(BaseModel):
    """Assistant payload in Unisound format."""

    model_config = ConfigDict(extra="forbid")

    thought: str = ""
    answer: str = ""
    tool_calls: list[dict[str, Any]] = Field(default_factory=list)


class UnisoundResponseProgress(BaseModel):
    """Mapped assistant response feedback."""

    model_config = ConfigDict(extra="allow")

    response_index: int
    episode_index: int
    response_progress: str | None = None
    llm_error: bool = False
    feedback_kind: str
    feedback_message_start_index: int | None = None
    feedback_message_end_index: int | None = None


class UnisoundUserSatisfiedEpisode(BaseModel):
    """Mapped user episode feedback."""

    model_config = ConfigDict(extra="allow")

    episode_index: int
    user_satisfied: str | None = None
    llm_error: bool = False
    message_start_index: int
    message_end_index: int


class UnisoundTurnRoundFeedback(BaseModel):
    """Round feedback attached to a rebuilt Unisound turn."""

    model_config = ConfigDict(extra="forbid")

    response_progress: UnisoundResponseProgress | None = None
    user_satisfied_episode: UnisoundUserSatisfiedEpisode | None = None


class UnisoundDialogTurn(BaseModel):
    """Single Unisound dialog turn."""

    model_config = ConfigDict(extra="forbid")

    turn_id: int
    loss: bool = True
    User: str | None = None
    Tool: list[dict[str, Any]] = Field(default_factory=list)
    Assistant: UnisoundAssistant
    round_feedback: UnisoundTurnRoundFeedback | None = None

    @model_validator(mode="after")
    def validate_anchor(self) -> "UnisoundDialogTurn":
        if not (self.User or self.Tool):
            raise ValueError("each dialog turn must contain User or Tool")
        return self


class UnisoundRecord(BaseModel):
    """Validated Unisound export record."""

    model_config = ConfigDict(extra="allow")

    id: str
    domain: str
    task_describe: str
    data_source: str
    Chosen: str
    Rejected: str
    system_prompt: str = ""
    tools: list[dict[str, Any]] = Field(default_factory=list)
    dialog: list[UnisoundDialogTurn] = Field(min_length=1)
    ext: dict[str, Any] | None = None

    @model_validator(mode="after")
    def validate_chosen_rejected(self) -> "UnisoundRecord":
        last_turn = self.dialog[-1]
        available_keys = {"Assistant"}
        if self.Chosen not in available_keys or self.Rejected not in available_keys:
            raise ValueError("Chosen and Rejected must exist in the last dialog turn")
        return self


class UnisoundExportConfig(BaseModel):
    """Config used by the offline converter."""

    model_config = ConfigDict(extra="forbid")

    domain: str
    task_describe: str
    data_source: str
    default_answer_key: str = "Assistant"
    id_strategy: str = "source_metadata_then_sample_uid"
    preserve_extensions: bool = True
    preserve_round_feedback: bool = True
    preserve_conversation: bool = True
    task_describe_en_suffix: bool = True
    turn_feedback_field: str = "round_feedback"
    think_split_strategy: str = "tag"
    english_detection_mode: str = "simple"
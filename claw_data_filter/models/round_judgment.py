from enum import Enum
from typing import Optional
from datetime import datetime
from pydantic import BaseModel, Field


class JudgmentValue(str, Enum):
    YES = "yes"
    NO = "no"
    UNCERTAIN = "uncertain"
    NEUTRAL = "neutral"  # For user_satisfied only


class RoundJudgment(BaseModel):
    """Deprecated single-layer judgment model kept for transitional compatibility."""

    sample_id: int
    turn_index: int
    response_helpful: Optional[str] = None  # yes/no/uncertain
    user_satisfied: Optional[str] = None    # yes/no/uncertain/neutral
    signal_from_users: list[str] = Field(default_factory=list)
    llm_error: bool = False
    created_at: datetime = Field(default_factory=lambda: datetime.now())


class FeedbackKind(str, Enum):
    TOOL_RESULT = "tool_result"
    USER = "user"
    NONE = "none"


class AssistantResponseJudgment(BaseModel):
    """Judgment for a single assistant response unit."""

    sample_uid: str
    response_index: int
    episode_index: int
    assistant_message_index: int
    feedback_kind: FeedbackKind = FeedbackKind.NONE
    feedback_message_start_index: Optional[int] = None
    feedback_message_end_index: Optional[int] = None
    feedback_payload: list[str] = Field(default_factory=list)
    response_helpful: Optional[str] = None
    llm_error: bool = False
    created_at: datetime = Field(default_factory=lambda: datetime.now())
    judgment_uid: str = ""

    def model_post_init(self, __context) -> None:
        if not self.judgment_uid:
            self.judgment_uid = f"resp:{self.sample_uid}:{self.response_index}"


class UserEpisodeJudgment(BaseModel):
    """Judgment for a complete user episode."""

    sample_uid: str
    episode_index: int
    start_user_message_index: int
    end_before_user_message_index: Optional[int] = None
    signal_from_users: list[str] = Field(default_factory=list)
    user_satisfied: Optional[str] = None
    llm_error: bool = False
    created_at: datetime = Field(default_factory=lambda: datetime.now())
    judgment_uid: str = ""

    def model_post_init(self, __context) -> None:
        if not self.judgment_uid:
            self.judgment_uid = f"episode:{self.sample_uid}:{self.episode_index}"

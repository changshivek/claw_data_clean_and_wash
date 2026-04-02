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
    """单轮判断结果（简化版）"""

    sample_id: int
    turn_index: int
    response_helpful: Optional[str] = None  # yes/no/uncertain
    user_satisfied: Optional[str] = None    # yes/no/uncertain/neutral
    signal_from_users: list[str] = Field(default_factory=list)
    llm_error: bool = False
    created_at: datetime = Field(default_factory=lambda: datetime.now())

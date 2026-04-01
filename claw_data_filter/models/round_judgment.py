from enum import Enum
from typing import Optional
from datetime import datetime
from pydantic import BaseModel, Field


class JudgmentValue(str, Enum):
    YES = "yes"
    NO = "no"
    UNCERTAIN = "uncertain"


class RoundJudgment(BaseModel):
    """单轮判断结果"""

    sample_id: int
    turn_index: int
    need_tool: str = Field(default="uncertain")  # yes/no/uncertain
    tool_correct: Optional[str] = None  # yes/no/uncertain/null when error
    response_helpful: Optional[str] = None  # yes/no/uncertain/null when error
    user_satisfied: Optional[str] = None  # yes/no/uncertain/neutral/null when error
    signal_from_users: list[str] = Field(default_factory=list)
    llm_error: bool = False
    created_at: datetime = Field(default_factory=lambda: datetime.now())
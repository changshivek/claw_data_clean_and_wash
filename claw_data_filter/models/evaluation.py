"""Evaluation model for LLM assessment results."""
from typing import Optional
from datetime import datetime
from pydantic import BaseModel, Field, field_validator


class Evaluation(BaseModel):
    """Represents evaluation result for a sample."""

    id: Optional[int] = None
    sample_id: int
    task_type: str = "unknown"
    progress_score: int = Field(default=0)
    tool_quality_score: float = Field(ge=0.0, le=1.0)
    tool_success_rate: float = Field(ge=0.0, le=1.0)
    overall_score: float = Field(ge=0.0, le=10.0)
    reasoning: str = ""
    evaluated_at: datetime = Field(default_factory=datetime.now)

    @field_validator("progress_score")
    @classmethod
    def validate_progress(cls, v: int) -> int:
        # Allow all values 0-5, clamp out-of-range to nearest valid value
        if v < 0:
            return 0
        if v > 5:
            return 5
        return v
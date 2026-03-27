"""Evaluation model for LLM assessment results."""
from typing import Optional
from datetime import datetime
from pydantic import BaseModel, Field, field_validator


class Evaluation(BaseModel):
    """Represents evaluation result for a sample."""

    id: Optional[int] = None
    sample_id: int
    task_type: str = "unknown"
    progress_score: int = Field(ge=0, le=5)
    tool_quality_score: float = Field(ge=0.0, le=1.0)
    tool_success_rate: float = Field(ge=0.0, le=1.0)
    overall_score: float = Field(ge=0.0, le=10.0)
    reasoning: str = ""
    evaluated_at: datetime = Field(default_factory=datetime.now)

    @field_validator("progress_score")
    @classmethod
    def validate_progress(cls, v: int) -> int:
        if v not in (0, 1, 2, 4, 5):  # 3 is reserved per spec
            raise ValueError(f"Invalid progress_score: {v}. Must be 0, 1, 2, 4, or 5.")
        return v
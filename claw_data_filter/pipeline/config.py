"""Configuration models for the incremental pipeline service."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python < 3.11
    import tomli as tomllib  # type: ignore


def _shared_cpu_budget(max_cap: int | None = None) -> int:
    cpu_count = max(1, os.cpu_count() or 1)
    budget = max(1, int(cpu_count * 0.7))
    if max_cap is not None:
        budget = min(budget, max_cap)
    return budget


class PathSettings(BaseModel):
    source_dir: Path
    unpack_dir: Path
    work_dir: Path
    db_path: Path
    export_dir: Path
    log_dir: Path


class ImportSettings(BaseModel):
    workers: int = Field(default_factory=lambda: _shared_cpu_budget(max_cap=8))
    chunk_size: int = 64
    skip_errors: bool = True


class SessionMergeSettings(BaseModel):
    enabled: bool = True
    workers: int = Field(default_factory=lambda: _shared_cpu_budget(max_cap=16))
    batch_size: int = 512
    min_prefix_turns: int = 2


class RoundFeedbackSettings(BaseModel):
    enabled: bool = True
    workers: int = 10
    batch_size: int = 10


class LLMSettings(BaseModel):
    endpoint: str = "http://localhost:8000/v1"
    api_key: str | None = None
    model_id: str | None = None
    timeout: float = 60.0
    max_retries: int = 3
    retry_base_delay: float = 5.0
    retry_max_delay: float = 30.0


class ExportSettings(BaseModel):
    response_progress_rate: str | None = ">=0.7"
    user_satisfied_rate: str | None = None
    user_negative_feedback_rate: str | None = None
    empty_response: bool | None = False
    num_turns_min: int | None = 3
    session_merge_keep: bool | None = True
    session_merge_status: str | None = None
    has_error: bool | None = False
    keep_intermediate: bool = True
    unisound_config_path: Path


class ScheduleSettings(BaseModel):
    cron: str = "*/30 * * * *"


class WebSettings(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8501


class PipelineConfig(BaseModel):
    paths: PathSettings
    llm: LLMSettings = Field(default_factory=LLMSettings)
    import_settings: ImportSettings = Field(default_factory=ImportSettings, alias="import")
    session_merge: SessionMergeSettings = Field(default_factory=SessionMergeSettings)
    round_feedback: RoundFeedbackSettings = Field(default_factory=RoundFeedbackSettings)
    export: ExportSettings
    schedule: ScheduleSettings = Field(default_factory=ScheduleSettings)
    web: WebSettings = Field(default_factory=WebSettings)

    def model_post_init(self, __context: Any) -> None:
        if self.llm.api_key is None:
            self.llm.api_key = os.getenv("LLM_API_KEY")
        if self.llm.model_id is None:
            self.llm.model_id = os.getenv("LLM_MODEL_ID")
        if not self.llm.endpoint:
            self.llm.endpoint = os.getenv("LLM_ENDPOINT", "http://localhost:8000/v1")

    @classmethod
    def from_toml(cls, config_path: Path) -> "PipelineConfig":
        base_dir = config_path.resolve().parent
        payload = tomllib.loads(config_path.read_text(encoding="utf-8"))
        resolved = cls._resolve_paths(payload, base_dir)
        return cls.model_validate(resolved)

    @staticmethod
    def _resolve_paths(payload: dict[str, Any], base_dir: Path) -> dict[str, Any]:
        copied = dict(payload)
        for section_name in ("paths", "export"):
            section = dict(copied.get(section_name, {}))
            for key, value in list(section.items()):
                if not isinstance(value, str):
                    continue
                if key.endswith("_dir") or key.endswith("_path") or key == "db_path":
                    path = Path(value).expanduser()
                    if not path.is_absolute():
                        path = (base_dir / path).resolve()
                    section[key] = path
            copied[section_name] = section
        return copied
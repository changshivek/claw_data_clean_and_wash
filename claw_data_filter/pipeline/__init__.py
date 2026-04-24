"""Incremental pipeline orchestration for tar-based data sources."""

from __future__ import annotations

from typing import Any

__all__ = ["PipelineConfig", "PipelineService"]


def __getattr__(name: str) -> Any:
	if name == "PipelineConfig":
		from claw_data_filter.pipeline.config import PipelineConfig

		return PipelineConfig
	if name == "PipelineService":
		from claw_data_filter.pipeline.service import PipelineService

		return PipelineService
	raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
"""Configuration management."""
import os
from pathlib import Path
from typing import Optional
from pydantic import BaseModel


class Config(BaseModel):
    llm_endpoint: str = "http://localhost:8000/v1"
    llm_api_key: Optional[str] = None
    llm_model_id: Optional[str] = None
    db_path: Path = Path("./data.duckdb")
    worker_count: int = max(1, (os.cpu_count() or 1) // 2)
    batch_size: int = 10
    max_retries: int = 3
    # Round feedback specific
    max_concurrency: int = 10
    llm_timeout: float = 60.0
    context_window: int = 4096

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            llm_endpoint=os.getenv("LLM_ENDPOINT", "http://localhost:8000/v1"),
            llm_api_key=os.getenv("LLM_API_KEY"),
            llm_model_id=os.getenv("LLM_MODEL_ID"),
            db_path=Path(os.getenv("DB_PATH", "./data.duckdb")),
            worker_count=int(os.getenv("WORKER_COUNT", (os.cpu_count() or 1) // 2)),
            batch_size=int(os.getenv("BATCH_SIZE", 10)),
            max_retries=int(os.getenv("MAX_RETRIES", 3)),
            max_concurrency=int(os.getenv("MAX_CONCURRENCY", 10)),
            llm_timeout=float(os.getenv("LLM_TIMEOUT", 60.0)),
            context_window=int(os.getenv("CONTEXT_WINDOW", 4096)),
        )

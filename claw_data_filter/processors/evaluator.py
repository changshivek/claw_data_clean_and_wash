"""LLM-based evaluation processor."""
import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from claw_data_filter.config import Config
from claw_data_filter.models.evaluation import Evaluation
from claw_data_filter.models.sample import Sample
from claw_data_filter.processors.formatter import ConversationFormatter
from claw_data_filter.prompts.evaluation_prompt import build_evaluation_prompt
from claw_data_filter.storage.duckdb_store import DuckDBStore

logger = logging.getLogger(__name__)


class EvaluationError(Exception):
    """Raised when evaluation fails."""


class Evaluator:
    """Evaluate conversation samples using LLM."""

    def __init__(self, store: DuckDBStore, config: Config):
        self.store = store
        self.config = config
        self.formatter = ConversationFormatter()

        # Import here to avoid circular dependency
        from claw_data_filter.llm.client import LLMClient
        self.llm = LLMClient(
            endpoint=config.llm_endpoint,
            api_key=config.llm_api_key,
            max_retries=config.max_retries,
        )

    def _parse_evaluation_response(self, raw_response: str, sample_id: int) -> Evaluation:
        """Parse LLM JSON response into Evaluation model.

        Args:
            raw_response: Raw text response from LLM
            sample_id: ID of the sample being evaluated

        Returns:
            Evaluation model instance

        Raises:
            EvaluationError: If JSON cannot be extracted or parsed
        """
        # Try to extract JSON from response (may have surrounding text)
        json_match = re.search(r'\{[^{}]*"[^{}]*\}', raw_response, re.DOTALL)
        if json_match:
            json_str = json_match.group()
        else:
            # Try simpler approach - find first { and last }
            start = raw_response.find("{")
            end = raw_response.rfind("}") + 1
            if start != -1 and end != 0:
                json_str = raw_response[start:end]
            else:
                raise EvaluationError(f"Could not find JSON in response for sample {sample_id}")

        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as e:
            raise EvaluationError(f"Invalid JSON in response for sample {sample_id}: {e}")

        return Evaluation(
            sample_id=sample_id,
            task_type=data.get("task_type", "unknown"),
            progress_score=data.get("progress_score", 0),
            tool_quality_score=data.get("tool_quality_score", 0.0),
            tool_success_rate=data.get("tool_success_rate", 0.0),
            overall_score=data.get("overall_score", 0.0),
            reasoning=data.get("reasoning", ""),
        )

    def evaluate_sample(self, sample_id: int, sample: Sample) -> Evaluation:
        """Evaluate a single sample.

        Args:
            sample_id: ID of the sample
            sample: Sample model instance

        Returns:
            Evaluation result

        Raises:
            EvaluationError: If LLM call fails or response cannot be parsed
        """
        formatted = self.formatter.format(sample.raw_json)
        system_prompt, user_prompt = build_evaluation_prompt(formatted)

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        response = self.llm.chat(messages)
        evaluation = self._parse_evaluation_response(response, sample_id)
        self.store.insert_evaluation(evaluation)

        return evaluation

    def evaluate_batch(self, workers: Optional[int] = None) -> tuple[int, int]:
        """Evaluate all unevaluated samples using parallel workers.

        Args:
            workers: Number of parallel workers (defaults to config.worker_count)

        Returns:
            Tuple of (success_count, failure_count)
        """
        if workers is None:
            workers = self.config.worker_count

        success = 0
        failures = 0

        while True:
            # Fetch batch of unevaluated
            batch = self.store.get_unevaluated_samples(limit=self.config.batch_size)
            if not batch:
                break

            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(self.evaluate_sample, sample_id, sample): sample_id
                    for sample_id, sample in batch
                }

                for future in as_completed(futures):
                    sample_id = futures[future]
                    try:
                        future.result()
                        success += 1
                        logger.info(f"Evaluated sample {sample_id}")
                    except Exception as e:
                        failures += 1
                        logger.error(f"Failed to evaluate sample {sample_id}: {e}")

        logger.info(f"Evaluation complete: {success} success, {failures} failures")
        return success, failures

    def close(self):
        """Close resources."""
        self.llm.close()
"""Convert openai_round_feedback_v2 JSONL into Unisound format with validation."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Iterable

from pydantic import ValidationError

try:
    from scripts.unisound_export_models import (
        OpenAIRoundFeedbackRecord,
        ResponseProgressStep,
        UnisoundAssistant,
        UnisoundDialogTurn,
        UnisoundExportConfig,
        UnisoundRecord,
        UnisoundResponseProgress,
        UnisoundTurnRoundFeedback,
        UnisoundUserSatisfiedEpisode,
        UserSatisfiedEpisode,
    )
except ModuleNotFoundError:
    from unisound_export_models import (  # type: ignore
        OpenAIRoundFeedbackRecord,
        ResponseProgressStep,
        UnisoundAssistant,
        UnisoundDialogTurn,
        UnisoundExportConfig,
        UnisoundRecord,
        UnisoundResponseProgress,
        UnisoundTurnRoundFeedback,
        UnisoundUserSatisfiedEpisode,
        UserSatisfiedEpisode,
    )


THINK_PATTERN = re.compile(r"<think>(.*?)</think>", re.DOTALL | re.IGNORECASE)
THINK_TAG_PATTERN = re.compile(r"</?think>", re.IGNORECASE)


class EmptyDialogError(ValueError):
    """Raised when a record cannot be rebuilt into a valid Unisound dialog."""


def load_config(config_path: Path) -> UnisoundExportConfig:
    """Load JSON config used for conversion."""
    return UnisoundExportConfig.model_validate_json(config_path.read_text(encoding="utf-8"))


def iter_validated_input_records(input_path: Path, limit: int | None = None) -> Iterable[OpenAIRoundFeedbackRecord]:
    """Yield validated input records from exported JSONL."""
    with input_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if limit is not None and line_number > limit:
                break
            if not line.strip():
                continue
            payload = json.loads(line)
            try:
                yield OpenAIRoundFeedbackRecord.model_validate(payload)
            except ValidationError as exc:
                raise ValueError(f"input validation failed at line {line_number}: {exc}") from exc


def validate_input_file(input_path: Path, limit: int | None = None) -> int:
    """Validate the input JSONL file and return record count."""
    count = 0
    for _ in iter_validated_input_records(input_path, limit=limit):
        count += 1
    return count


def validate_output_file(output_path: Path, limit: int | None = None) -> int:
    """Validate converted Unisound JSONL file and return record count."""
    count = 0
    with output_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if limit is not None and line_number > limit:
                break
            if not line.strip():
                continue
            payload = json.loads(line)
            try:
                UnisoundRecord.model_validate(payload)
            except ValidationError as exc:
                raise ValueError(f"output validation failed at line {line_number}: {exc}") from exc
            count += 1
    return count


def convert_file(input_path: Path, output_path: Path, config: UnisoundExportConfig, limit: int | None = None) -> dict[str, Any]:
    """Convert validated records to Unisound JSONL and return a summary report."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    english_count = 0
    sample_uids: list[str] = []
    skipped_records: list[dict[str, str]] = []

    with output_path.open("w", encoding="utf-8") as handle:
        for record in iter_validated_input_records(input_path, limit=limit):
            try:
                converted = convert_record(record, config)
            except EmptyDialogError as exc:
                skipped_records.append(
                    {
                        "sample_uid": record.metadata.sample_uid,
                        "reason": str(exc),
                    }
                )
                continue
            if converted.task_describe.endswith("-en"):
                english_count += 1
            sample_uids.append(record.metadata.sample_uid)
            handle.write(converted.model_dump_json(ensure_ascii=False) + "\n")
            count += 1

    return {
        "count": count,
        "english_count": english_count,
        "sample_uids": sample_uids,
        "skipped_count": len(skipped_records),
        "skipped_records": skipped_records,
        "output_path": str(output_path),
    }


def convert_record(record: OpenAIRoundFeedbackRecord, config: UnisoundExportConfig) -> UnisoundRecord:
    """Convert one validated OpenAI round feedback record to Unisound format."""
    response_by_assistant_index = {
        step.assistant_message_index: step for step in record.round_feedback.response_progress_steps
    }
    episode_by_index = {
        episode.episode_index: episode for episode in record.round_feedback.user_satisfied_episodes
    }
    episode_start_by_index = {
        episode.message_start_index: episode.episode_index for episode in record.round_feedback.user_satisfied_episodes
    }

    system_prompt, turns = _build_dialog_turns(
        record,
        response_by_assistant_index=response_by_assistant_index,
        episode_by_index=episode_by_index,
        episode_start_by_index=episode_start_by_index,
        default_answer_key=config.default_answer_key,
        turn_feedback_field=config.turn_feedback_field,
    )

    task_describe = config.task_describe
    if not turns:
        raise EmptyDialogError(
            f"sample_uid={record.metadata.sample_uid} has no user/tool anchored dialog turns"
        )
    if config.task_describe_en_suffix and _dialog_looks_english(turns):
        task_describe = f"{task_describe}-en"

    ext: dict[str, Any] | None = None
    if config.preserve_extensions:
        ext = {
            "metadata": record.metadata.model_dump(mode="json"),
            "source_metadata": record.source_metadata.model_dump(mode="json"),
        }
        if config.preserve_round_feedback:
            ext["round_feedback"] = record.round_feedback.model_dump(mode="json")
        if config.preserve_conversation:
            ext["conversation"] = record.conversation.model_dump(mode="json")

    output = {
        "id": _resolve_record_id(record, config),
        "domain": config.domain,
        "task_describe": task_describe,
        "data_source": config.data_source,
        "Chosen": config.default_answer_key,
        "Rejected": config.default_answer_key,
        "system_prompt": system_prompt,
        "tools": _build_unisound_tools(record.conversation.tools),
        "dialog": [turn.model_dump(mode="json") for turn in turns],
    }
    if ext:
        output["ext"] = ext

    try:
        return UnisoundRecord.model_validate(output)
    except ValidationError as exc:
        raise ValueError(f"output validation failed for sample_uid={record.metadata.sample_uid}: {exc}") from exc


def _build_dialog_turns(
    record: OpenAIRoundFeedbackRecord,
    response_by_assistant_index: dict[int, ResponseProgressStep],
    episode_by_index: dict[int, UserSatisfiedEpisode],
    episode_start_by_index: dict[int, int],
    default_answer_key: str,
    turn_feedback_field: str,
) -> tuple[str, list[UnisoundDialogTurn]]:
    system_parts: list[str] = []
    turns: list[UnisoundDialogTurn] = []
    pending_anchor: dict[str, Any] | None = None
    reusable_anchor: dict[str, Any] | None = None
    current_episode_index = -1
    next_episode_fallback = 0

    for message_index, message in enumerate(record.conversation.messages):
        if message.role in {"system", "developer"}:
            text = _extract_text_content(message.content)
            if text:
                system_parts.append(text)
            continue

        if message.role == "user":
            if message_index in episode_start_by_index:
                current_episode_index = episode_start_by_index[message_index]
                next_episode_fallback = max(next_episode_fallback, current_episode_index + 1)
            else:
                current_episode_index = next_episode_fallback
                next_episode_fallback += 1

            pending_anchor = {
                "kind": "User",
                "User": _extract_text_content(message.content),
                "episode_index": current_episode_index,
                "source_indices": [message_index],
            }
            reusable_anchor = pending_anchor
            continue

        if message.role == "tool":
            tool_payload = message.model_dump(mode="json", exclude_none=True)
            if pending_anchor and pending_anchor["kind"] == "Tool":
                pending_anchor["Tool"].append(tool_payload)
                pending_anchor["source_indices"].append(message_index)
            else:
                pending_anchor = {
                    "kind": "Tool",
                    "Tool": [tool_payload],
                    "episode_index": current_episode_index,
                    "source_indices": [message_index],
                }
            reusable_anchor = pending_anchor
            continue

        if message.role != "assistant":
            continue

        anchor = pending_anchor or reusable_anchor
        if anchor is None:
            anchor = {
                "kind": "User",
                "User": "",
                "episode_index": current_episode_index,
                "source_indices": [],
            }

        assistant_payload = _build_unisound_assistant(message.content, message.tool_calls)
        round_feedback = _build_turn_feedback(
            response_by_assistant_index.get(message_index),
            episode_by_index.get(anchor.get("episode_index", -1)),
        )

        turn_payload: dict[str, Any] = {
            "turn_id": len(turns) + 1,
            "loss": True,
            default_answer_key: assistant_payload.model_dump(mode="json"),
        }
        if anchor["kind"] == "User":
            turn_payload["User"] = anchor.get("User", "")
        else:
            turn_payload["Tool"] = anchor.get("Tool", [])
        if round_feedback is not None:
            turn_payload[turn_feedback_field] = round_feedback.model_dump(mode="json")

        turns.append(UnisoundDialogTurn.model_validate(turn_payload))
        pending_anchor = None
        reusable_anchor = anchor

    return "\n\n".join(part for part in system_parts if part) or "", turns


def _build_turn_feedback(
    response_step: ResponseProgressStep | None,
    user_episode: UserSatisfiedEpisode | None,
) -> UnisoundTurnRoundFeedback | None:
    if response_step is None and user_episode is None:
        return None
    return UnisoundTurnRoundFeedback(
        response_progress=None
        if response_step is None
        else UnisoundResponseProgress(
            response_index=response_step.response_index,
            episode_index=response_step.episode_index,
            response_progress=response_step.response_progress,
            llm_error=response_step.llm_error,
            feedback_kind=response_step.feedback_kind,
            feedback_message_start_index=response_step.feedback_message_start_index,
            feedback_message_end_index=response_step.feedback_message_end_index,
        ),
        user_satisfied_episode=None
        if user_episode is None
        else UnisoundUserSatisfiedEpisode(
            episode_index=user_episode.episode_index,
            user_satisfied=user_episode.user_satisfied,
            llm_error=user_episode.llm_error,
            message_start_index=user_episode.message_start_index,
            message_end_index=user_episode.message_end_index,
        ),
    )


def _build_unisound_tools(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Flatten OpenAI function tools into Unisound top-level tool items."""
    normalized: list[dict[str, Any]] = []
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        function_payload = tool.get("function")
        if tool.get("type") == "function" and isinstance(function_payload, dict):
            normalized_tool: dict[str, Any] = {}
            for key in ("name", "description", "parameters"):
                if key in function_payload:
                    normalized_tool[key] = function_payload[key]
            if normalized_tool:
                normalized.append(normalized_tool)
            continue
        normalized.append(tool)
    return normalized


def _build_unisound_assistant(content: Any, tool_calls: list[dict[str, Any]]) -> UnisoundAssistant:
    text = _extract_text_content(content)
    thought, answer = _split_thought_and_answer(text)
    return UnisoundAssistant(
        thought=thought,
        answer=answer,
        tool_calls=tool_calls or [],
    )


def _split_thought_and_answer(text: str) -> tuple[str, str]:
    if not text:
        return "", ""
    thoughts = [part.strip() for part in THINK_PATTERN.findall(text) if part.strip()]
    if not thoughts:
        lowered = text.lower()
        open_index = lowered.find("<think>")
        if open_index != -1:
            answer = text[:open_index].strip()
            thought = THINK_TAG_PATTERN.sub("", text[open_index:]).strip()
            return thought, answer
        return "", THINK_TAG_PATTERN.sub("", text).strip()
    answer = THINK_PATTERN.sub("", text).strip()
    answer = THINK_TAG_PATTERN.sub("", answer).strip()
    return "\n\n".join(thoughts), answer


def _extract_text_content(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for part in content:
            if isinstance(part, str):
                parts.append(part)
            elif isinstance(part, dict):
                if part.get("type") == "text":
                    parts.append(str(part.get("text", "")))
                elif "text" in part:
                    parts.append(str(part["text"]))
                elif "content" in part and isinstance(part["content"], str):
                    parts.append(part["content"])
        return "".join(parts).strip()
    if isinstance(content, dict):
        if isinstance(content.get("text"), str):
            return content["text"].strip()
        return json.dumps(content, ensure_ascii=False)
    return "" if content is None else str(content)


def _dialog_looks_english(turns: list[UnisoundDialogTurn]) -> bool:
    text_parts: list[str] = []
    for turn in turns:
        if turn.User:
            text_parts.append(turn.User)
            break
    text = " ".join(text_parts)
    if not text:
        return False
    ascii_letters = sum(1 for ch in text if ch.isascii() and ch.isalpha())
    letters = sum(1 for ch in text if ch.isalpha())
    return letters > 0 and ascii_letters / letters >= 0.8


def _resolve_record_id(record: OpenAIRoundFeedbackRecord, config: UnisoundExportConfig) -> str:
    metadata = record.source_metadata.metadata
    if config.id_strategy == "source_metadata_then_sample_uid" and isinstance(metadata, dict):
        for key in ("_id", "id"):
            value = metadata.get(key)
            if value not in (None, ""):
                return str(value)
    if record.metadata.sample_uid:
        return record.metadata.sample_uid
    if record.metadata.local_sample_id is not None:
        return str(record.metadata.local_sample_id)
    raise ValueError("unable to resolve output id")


def build_report(summary: dict[str, Any], report_path: Path) -> None:
    """Write conversion report JSON."""
    report_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    """CLI entry for validation and conversion."""
    parser = argparse.ArgumentParser(description="Convert openai_round_feedback_v2 JSONL to Unisound JSONL")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate_parser = subparsers.add_parser("validate-input", help="Validate input JSONL against the input model")
    validate_parser.add_argument("--input", required=True, type=Path)
    validate_parser.add_argument("--limit", type=int, default=None)

    validate_output_parser = subparsers.add_parser("validate-output", help="Validate converted Unisound JSONL")
    validate_output_parser.add_argument("--input", required=True, type=Path)
    validate_output_parser.add_argument("--limit", type=int, default=None)

    convert_parser = subparsers.add_parser("convert", help="Convert input JSONL into Unisound format")
    convert_parser.add_argument("--input", required=True, type=Path)
    convert_parser.add_argument("--output", required=True, type=Path)
    convert_parser.add_argument("--config", required=True, type=Path)
    convert_parser.add_argument("--limit", type=int, default=None)
    convert_parser.add_argument("--report", type=Path, default=None)

    args = parser.parse_args()

    if args.command == "validate-input":
        count = validate_input_file(args.input, limit=args.limit)
        print(json.dumps({"validated_records": count}, ensure_ascii=False))
        return

    if args.command == "validate-output":
        count = validate_output_file(args.input, limit=args.limit)
        print(json.dumps({"validated_records": count}, ensure_ascii=False))
        return

    config = load_config(args.config)
    summary = convert_file(args.input, args.output, config, limit=args.limit)
    if args.report is not None:
        build_report(summary, args.report)
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
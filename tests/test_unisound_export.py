"""Tests for Unisound offline conversion."""

from __future__ import annotations

import json
from pathlib import Path

from scripts.unisound_export import convert_file, convert_record, validate_input_file, validate_output_file
from scripts.unisound_export_models import OpenAIRoundFeedbackRecord, UnisoundExportConfig


def _config() -> UnisoundExportConfig:
    return UnisoundExportConfig(
        domain="Agent",
        task_describe="unit_test_dataset",
        data_source="unit_test_source",
        default_answer_key="Assistant",
        id_strategy="source_metadata_then_sample_uid",
        preserve_extensions=True,
        preserve_round_feedback=True,
        preserve_conversation=True,
        task_describe_en_suffix=True,
        turn_feedback_field="round_feedback",
    )


def _base_record() -> dict:
    return {
        "schema": "openai_round_feedback_v2",
        "metadata": {
            "sample_uid": "sample-1",
            "local_sample_id": 1,
            "imported_at": "2026-04-20T00:00:00",
        },
        "source_metadata": {
            "timestamp": None,
            "model_requested": "demo-model",
            "user_agent": "pytest",
            "request_id": None,
            "trace_id": None,
            "source_format": "openai",
            "metadata": {"id": "source-id-1"},
        },
        "conversation": {
            "messages": [
                {"role": "system", "content": "You are a coding assistant."},
                {"role": "user", "content": "Find recent papers."},
                {
                    "role": "assistant",
                    "content": "<think>Need search.</think>我先搜索一下。",
                    "tool_calls": [
                        {
                            "id": "call_1",
                            "type": "function",
                            "function": {
                                "name": "web_search",
                                "arguments": "{\"q\": \"papers\"}",
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_1",
                    "content": "{\"result\": [\"paper-1\"]}",
                },
                {
                    "role": "assistant",
                    "content": "<think>Need summarize.</think>这是整理后的结果。",
                },
            ],
            "tools": [
                {
                    "type": "function",
                    "function": {
                        "name": "web_search",
                        "description": "Search the web",
                        "parameters": {"type": "object", "properties": {"q": {"type": "string"}}},
                    },
                }
            ],
        },
        "round_feedback": {
            "response_progress_steps": [
                {
                    "response_index": 0,
                    "episode_index": 0,
                    "assistant_message_index": 2,
                    "feedback_kind": "tool_result",
                    "feedback_message_start_index": 3,
                    "feedback_message_end_index": 3,
                    "feedback_payload": ["paper-1"],
                    "response_progress": "yes",
                    "llm_error": False,
                },
                {
                    "response_index": 1,
                    "episode_index": 0,
                    "assistant_message_index": 4,
                    "feedback_kind": "none",
                    "feedback_message_start_index": None,
                    "feedback_message_end_index": None,
                    "feedback_payload": [],
                    "response_progress": "yes",
                    "llm_error": False,
                },
            ],
            "user_satisfied_episodes": [
                {
                    "episode_index": 0,
                    "message_start_index": 1,
                    "message_end_index": 4,
                    "signal_from_users": [],
                    "user_satisfied": "uncertain",
                    "llm_error": False,
                }
            ],
        },
    }


def test_convert_record_separates_top_level_fields_and_maps_feedback():
    record = OpenAIRoundFeedbackRecord.model_validate(_base_record())
    converted = convert_record(record, _config())

    assert converted.id == "source-id-1"
    assert converted.system_prompt == "You are a coding assistant."
    assert len(converted.tools) == 1
    assert converted.tools[0] == {
        "name": "web_search",
        "description": "Search the web",
        "parameters": {"type": "object", "properties": {"q": {"type": "string"}}},
    }
    assert len(converted.dialog) == 2
    assert converted.dialog[0].User == "Find recent papers."
    assert converted.dialog[0].Assistant.thought == "Need search."
    assert converted.dialog[0].Assistant.answer == "我先搜索一下。"
    assert converted.dialog[0].round_feedback is not None
    assert converted.dialog[0].round_feedback.response_progress is not None
    assert converted.dialog[1].Tool[0]["role"] == "tool"
    assert converted.dialog[1].Assistant.thought == "Need summarize."
    assert converted.dialog[1].round_feedback is not None
    assert converted.dialog[1].round_feedback.user_satisfied_episode is not None
    assert converted.dialog[1].round_feedback.user_satisfied_episode.episode_index == 0


def test_convert_record_duplicates_anchor_for_consecutive_assistant_messages():
    payload = _base_record()
    payload["conversation"]["messages"] = [
        {"role": "user", "content": "hello"},
        {"role": "assistant", "content": "first"},
        {"role": "assistant", "content": "second"},
    ]
    payload["round_feedback"] = {
        "response_progress_steps": [
            {
                "response_index": 0,
                "episode_index": 0,
                "assistant_message_index": 1,
                "feedback_kind": "none",
                "feedback_message_start_index": None,
                "feedback_message_end_index": None,
                "feedback_payload": [],
                "response_progress": "yes",
                "llm_error": False,
            },
            {
                "response_index": 1,
                "episode_index": 0,
                "assistant_message_index": 2,
                "feedback_kind": "none",
                "feedback_message_start_index": None,
                "feedback_message_end_index": None,
                "feedback_payload": [],
                "response_progress": "yes",
                "llm_error": False,
            },
        ],
        "user_satisfied_episodes": [
            {
                "episode_index": 0,
                "message_start_index": 0,
                "message_end_index": 2,
                "signal_from_users": [],
                "user_satisfied": "yes",
                "llm_error": False,
            }
        ],
    }

    record = OpenAIRoundFeedbackRecord.model_validate(payload)
    converted = convert_record(record, _config())

    assert len(converted.dialog) == 2
    assert converted.dialog[0].User == "hello"
    assert converted.dialog[1].User == "hello"
    assert converted.dialog[1].Assistant.answer == "second"


def test_validate_input_and_convert_file(tmp_path: Path):
    input_path = tmp_path / "input.jsonl"
    output_path = tmp_path / "output.jsonl"
    report_path = tmp_path / "report.json"
    input_path.write_text(json.dumps(_base_record(), ensure_ascii=False) + "\n", encoding="utf-8")

    validated_count = validate_input_file(input_path)
    summary = convert_file(input_path, output_path, _config())
    validated_output_count = validate_output_file(output_path)
    report_path.write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")

    assert validated_count == 1
    assert summary["count"] == 1
    assert validated_output_count == 1
    payload = json.loads(output_path.read_text(encoding="utf-8").splitlines()[0])
    assert payload["Chosen"] == "Assistant"
    assert payload["Rejected"] == "Assistant"
    assert payload["tools"][0]["name"] == "web_search"
    assert "function" not in payload["tools"][0]
    assert payload["dialog"][1]["Tool"][0]["tool_call_id"] == "call_1"


def test_convert_record_merges_developer_into_system_prompt():
    payload = _base_record()
    payload["conversation"]["messages"] = [
        {"role": "developer", "content": "Developer policy."},
        {"role": "user", "content": "hello"},
        {"role": "assistant", "content": "world"},
    ]
    payload["round_feedback"] = {
        "response_progress_steps": [
            {
                "response_index": 0,
                "episode_index": 0,
                "assistant_message_index": 2,
                "feedback_kind": "none",
                "feedback_message_start_index": None,
                "feedback_message_end_index": None,
                "feedback_payload": [],
                "response_progress": "yes",
                "llm_error": False,
            }
        ],
        "user_satisfied_episodes": [
            {
                "episode_index": 0,
                "message_start_index": 1,
                "message_end_index": 2,
                "signal_from_users": [],
                "user_satisfied": "yes",
                "llm_error": False,
            }
        ],
    }

    record = OpenAIRoundFeedbackRecord.model_validate(payload)
    converted = convert_record(record, _config())

    assert converted.system_prompt == "Developer policy."


def test_convert_record_keeps_empty_answer_when_message_is_think_only():
    payload = _base_record()
    payload["conversation"]["messages"] = [
        {"role": "user", "content": "hello"},
        {"role": "assistant", "content": "<think>internal reasoning only</think>"},
    ]
    payload["round_feedback"] = {
        "response_progress_steps": [
            {
                "response_index": 0,
                "episode_index": 0,
                "assistant_message_index": 1,
                "feedback_kind": "none",
                "feedback_message_start_index": None,
                "feedback_message_end_index": None,
                "feedback_payload": [],
                "response_progress": "yes",
                "llm_error": False,
            }
        ],
        "user_satisfied_episodes": [
            {
                "episode_index": 0,
                "message_start_index": 0,
                "message_end_index": 1,
                "signal_from_users": [],
                "user_satisfied": "yes",
                "llm_error": False,
            }
        ],
    }

    record = OpenAIRoundFeedbackRecord.model_validate(payload)
    converted = convert_record(record, _config())

    assert converted.dialog[0].Assistant.thought == "internal reasoning only"
    assert converted.dialog[0].Assistant.answer == ""


def test_convert_record_supports_unclosed_think_tag():
    payload = _base_record()
    payload["conversation"]["messages"] = [
        {"role": "user", "content": "hello"},
        {"role": "assistant", "content": "<think>reasoning without close"},
    ]
    payload["round_feedback"] = {
        "response_progress_steps": [
            {
                "response_index": 0,
                "episode_index": 0,
                "assistant_message_index": 1,
                "feedback_kind": "none",
                "feedback_message_start_index": None,
                "feedback_message_end_index": None,
                "feedback_payload": [],
                "response_progress": "yes",
                "llm_error": False,
            }
        ],
        "user_satisfied_episodes": [
            {
                "episode_index": 0,
                "message_start_index": 0,
                "message_end_index": 1,
                "signal_from_users": [],
                "user_satisfied": "yes",
                "llm_error": False,
            }
        ],
    }

    record = OpenAIRoundFeedbackRecord.model_validate(payload)
    converted = convert_record(record, _config())

    assert converted.dialog[0].Assistant.thought == "reasoning without close"
    assert converted.dialog[0].Assistant.answer == ""


def test_convert_record_strips_stray_think_tags_from_answer():
    payload = _base_record()
    payload["conversation"]["messages"] = [
        {"role": "user", "content": "hello"},
        {"role": "assistant", "content": "final answer </think> only"},
    ]
    payload["round_feedback"] = {
        "response_progress_steps": [
            {
                "response_index": 0,
                "episode_index": 0,
                "assistant_message_index": 1,
                "feedback_kind": "none",
                "feedback_message_start_index": None,
                "feedback_message_end_index": None,
                "feedback_payload": [],
                "response_progress": "yes",
                "llm_error": False,
            }
        ],
        "user_satisfied_episodes": [
            {
                "episode_index": 0,
                "message_start_index": 0,
                "message_end_index": 1,
                "signal_from_users": [],
                "user_satisfied": "yes",
                "llm_error": False,
            }
        ],
    }

    record = OpenAIRoundFeedbackRecord.model_validate(payload)
    converted = convert_record(record, _config())

    assert converted.dialog[0].Assistant.thought == ""
    assert converted.dialog[0].Assistant.answer == "final answer  only"
"""Build detail-page view models from storage records."""
from claw_data_filter.models.round_judgment import RoundJudgment
from claw_data_filter.models.sample import extract_messages_from_payload
from claw_data_filter.processors.round_feedback import TurnContextBuilder
from claw_data_filter.web.view_models.sample_detail_view import SampleDetailView, TurnDetailView


def build_sample_detail_view(
    sample_record: dict,
    judgments: list[RoundJudgment],
) -> SampleDetailView:
    """Build a detail-page view model from a sample record and its judgments."""
    tool_stats = sample_record.get("tool_stats") or {}
    messages = extract_messages_from_payload(sample_record.get("raw_json") or {})
    turn_contexts = TurnContextBuilder().extract_turns(messages)
    judgments_by_turn = {judgment.turn_index: judgment for judgment in judgments}

    turns: list[TurnDetailView] = []
    for turn in turn_contexts:
        judgment = judgments_by_turn.get(turn.turn_index)
        turns.append(
            TurnDetailView(
                turn_index=turn.turn_index,
                user_message=turn.user_message,
                assistant_message=turn.assistant_message,
                tool_calls=turn.tool_calls,
                tool_result=turn.tool_result,
                response_helpful=judgment.response_helpful if judgment else None,
                user_satisfied=judgment.user_satisfied if judgment else None,
                signal_from_users=judgment.signal_from_users if judgment else list(turn.signal_users),
                llm_error=judgment.llm_error if judgment else False,
            )
        )

    return SampleDetailView(
        sample_id=sample_record["id"],
        sample_uid=sample_record.get("sample_uid") or "-",
        num_turns=sample_record.get("num_turns") or 0,
        expected_judgment_count=sample_record.get("expected_judgment_count") or len(turns),
        num_tool_calls=sample_record.get("num_tool_calls") or 0,
        helpful_rate=tool_stats.get("response_helpful_rate", 0.0),
        satisfied_rate=tool_stats.get("user_satisfied_rate", 0.0),
        processing_status=sample_record.get("processing_status") or "pending",
        turns=turns,
    )

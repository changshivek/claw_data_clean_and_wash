"""Build detail-page view models from storage records."""
from claw_data_filter.models.round_judgment import AssistantResponseJudgment, UserEpisodeJudgment
from claw_data_filter.processors.round_feedback import TurnContextBuilder
from claw_data_filter.web.view_models.sample_detail_view import (
    EpisodeDetailView,
    ResponseStepDetailView,
    SampleDetailView,
)


def build_sample_detail_view(
    sample_record: dict,
    response_judgments: list[AssistantResponseJudgment],
    episode_judgments: list[UserEpisodeJudgment],
) -> SampleDetailView:
    """Build a detail-page view model from a sample record and its judgments."""
    tool_stats = sample_record.get("tool_stats") or {}
    messages = sample_record.get("normalized_messages") or []
    builder = TurnContextBuilder()
    response_contexts = builder.extract_response_contexts(sample_record.get("sample_uid") or "-", messages)
    episode_contexts = builder.extract_episode_contexts(sample_record.get("sample_uid") or "-", messages)
    response_by_index = {judgment.response_index: judgment for judgment in response_judgments}
    episode_by_index = {judgment.episode_index: judgment for judgment in episode_judgments}

    response_steps: list[ResponseStepDetailView] = []
    for context in response_contexts:
        judgment = response_by_index.get(context.response_index)
        response_steps.append(
            ResponseStepDetailView(
                response_index=context.response_index,
                episode_index=context.episode_index,
                assistant_message_index=context.assistant_message_index,
                user_message=context.user_message,
                assistant_message=context.assistant_message,
                tool_calls=context.tool_calls,
                feedback_kind=context.feedback_kind.value,
                feedback_message_start_index=context.feedback_message_start_index,
                feedback_message_end_index=context.feedback_message_end_index,
                feedback_payload=judgment.feedback_payload if judgment else list(context.feedback_payload),
                response_progress=judgment.response_progress if judgment else None,
                llm_error=judgment.llm_error if judgment else False,
            )
        )

    user_episodes: list[EpisodeDetailView] = []
    for context in episode_contexts:
        judgment = episode_by_index.get(context.episode_index)
        user_episodes.append(
            EpisodeDetailView(
                episode_index=context.episode_index,
                start_user_message_index=context.start_user_message_index,
                end_before_user_message_index=context.end_before_user_message_index,
                user_message=context.user_message,
                assistant_messages=list(context.assistant_messages),
                tool_calls=list(context.tool_calls),
                tool_results=list(context.tool_results),
                signal_from_users=judgment.signal_from_users if judgment else list(context.signal_from_users),
                user_satisfied=judgment.user_satisfied if judgment else None,
                llm_error=judgment.llm_error if judgment else False,
            )
        )

    return SampleDetailView(
        sample_id=sample_record["id"],
        sample_uid=sample_record.get("sample_uid") or "-",
        empty_response=bool(sample_record.get("empty_response")),
        num_turns=sample_record.get("num_turns") or 0,
        expected_judgment_count=(
            sample_record.get("expected_judgment_count")
            if sample_record.get("expected_judgment_count") is not None
            else (len(response_steps) + len(user_episodes))
        ),
        expected_response_judgment_count=(
            sample_record.get("expected_response_judgment_count")
            if sample_record.get("expected_response_judgment_count") is not None
            else len(response_steps)
        ),
        expected_episode_judgment_count=(
            sample_record.get("expected_episode_judgment_count")
            if sample_record.get("expected_episode_judgment_count") is not None
            else len(user_episodes)
        ),
        num_tool_calls=sample_record.get("num_tool_calls") or 0,
        progress_rate=tool_stats.get("response_progress_rate", 0.0),
        regress_rate=tool_stats.get("response_regress_rate", 0.0),
        satisfied_rate=tool_stats.get("user_satisfied_rate", 0.0),
        processing_status=sample_record.get("processing_status") or "pending",
        session_merge_status=sample_record.get("session_merge_status"),
        session_merge_keep=sample_record.get("session_merge_keep"),
        session_merge_group_id=sample_record.get("session_merge_group_id"),
        session_merge_group_size=sample_record.get("session_merge_group_size"),
        session_merge_representative_uid=sample_record.get("session_merge_representative_uid"),
        session_merge_reason=sample_record.get("session_merge_reason"),
        response_steps=response_steps,
        user_episodes=user_episodes,
    )

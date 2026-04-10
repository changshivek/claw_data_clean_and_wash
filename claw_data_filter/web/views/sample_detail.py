"""Sample detail page - shows all turns for a sample."""
import streamlit as st

from claw_data_filter.storage.duckdb_store import DuckDBStore
from claw_data_filter.web.components.page_shell import render_page_header
from claw_data_filter.web.config import get_active_db_path
from claw_data_filter.web.services.detail_builder import build_sample_detail_view
from claw_data_filter.web.state.models import PAGE_LABELS, RouteState
from claw_data_filter.web.state.router import go_back


def _render_expandable_text(label: str, text: str, preview_limit: int, key: str) -> None:
    if not text:
        st.markdown(f"**{label}:** 无")
        return

    if len(text) <= preview_limit:
        st.markdown(f"**{label}:** {text}")
        return

    st.markdown(f"**{label}（预览）:** {text[:preview_limit]}...")
    with st.expander(f"展开{label}全文", expanded=False):
        st.text_area(label, value=text, height=220, key=key, disabled=True)


def _response_color(value: str | None) -> str:
    if value == "yes":
        return "green"
    if value == "uncertain":
        return "orange"
    return "red"


def _satisfied_color(value: str | None) -> str:
    if value == "yes":
        return "green"
    if value == "neutral":
        return "gray"
    if value == "uncertain":
        return "orange"
    return "red"


def render(route: RouteState):
    render_page_header(
        "Sample 详情",
        "查看样本级元数据、session merge 标记、empty response 状态，以及与 round feedback 同语义的逐轮对话上下文。",
        "Detail",
    )

    if route.sample_uid is None:
        st.error("未指定 sample_uid")
        return

    store = DuckDBStore(get_active_db_path(st.session_state), read_only=True)
    sample = store.get_sample_by_uid(route.sample_uid)

    if not sample:
        st.error(f"Sample {route.sample_uid} 不存在")
        store.close()
        return

    detail = build_sample_detail_view(
        sample,
        store.get_assistant_response_judgments(sample["sample_uid"]),
        store.get_user_episode_judgments(sample["sample_uid"]),
    )

    col1, col2, col3, col4, col5, col6, col7 = st.columns(7)
    col1.markdown(f"**sample_uid:** {detail.sample_uid}")
    col2.markdown(f"**episodes:** {detail.expected_episode_judgment_count}")
    col3.markdown(f"**response_steps:** {detail.expected_response_judgment_count}")
    col4.markdown(f"**stored_expected_judgments:** {detail.expected_judgment_count}")
    col5.markdown(f"**progress_rate:** {detail.progress_rate:.2f}")
    col6.markdown(f"**satisfied_rate:** {detail.satisfied_rate:.2f}")
    col7.markdown(f"**regress_rate:** {detail.regress_rate:.2f}")
    col8, _ = st.columns([1, 5])
    col8.markdown(f"**num_tool_calls:** {detail.num_tool_calls}")

    st.caption(f"local sample_id: {detail.sample_id}")
    st.caption(f"processing_status: {detail.processing_status}")
    st.caption(f"empty_response: {detail.empty_response}")
    st.caption(
        "session_merge: "
        f"status={detail.session_merge_status or 'unmarked'}, "
        f"keep={detail.session_merge_keep if detail.session_merge_keep is not None else 'n/a'}, "
        f"reason={detail.session_merge_reason or '-'}, "
        f"representative_uid={detail.session_merge_representative_uid or detail.sample_uid}, "
        f"group_size={detail.session_merge_group_size or '-'}"
    )

    st.divider()

    col_back, _ = st.columns([1, 4])
    back_label = PAGE_LABELS[route.back_target]
    if col_back.button(f"← 返回{back_label}"):
        go_back(st.query_params, route)
        st.rerun()

    st.markdown("### User Satisfied Episodes")

    if not detail.user_episodes and not detail.response_steps:
        if detail.empty_response:
            st.info("该样本已标记为 empty_response：导入数据中只有 user，没有 assistant。")
        else:
            st.info("没有找到对话记录")
        store.close()
        return

    if not detail.user_episodes:
        st.info("当前没有 user_satisfied episode judgment。")
    for episode in detail.user_episodes:
        satisfied = episode.user_satisfied or "-"
        with st.expander(
            f"Episode {episode.episode_index} | satisfied: :{_satisfied_color(satisfied)}[{satisfied}]"
        ):
            _render_expandable_text("起始 User", episode.user_message, 220, f"episode_{episode.episode_index}_user")
            st.markdown(
                f"**消息范围:** start={episode.start_user_message_index}, end={episode.end_before_user_message_index if episode.end_before_user_message_index is not None else '-'}"
            )
            if episode.assistant_messages:
                for idx, assistant_message in enumerate(episode.assistant_messages):
                    _render_expandable_text(
                        f"Assistant #{idx}",
                        assistant_message,
                        280,
                        f"episode_{episode.episode_index}_assistant_{idx}",
                    )
            if episode.tool_calls:
                tool_names = [call.get("name", "unknown") for call in episode.tool_calls]
                st.markdown(f"**Tool calls:** {', '.join(tool_names)}")
            if episode.tool_results:
                for idx, tool_result in enumerate(episode.tool_results):
                    _render_expandable_text(
                        f"Tool result #{idx}",
                        tool_result,
                        280,
                        f"episode_{episode.episode_index}_tool_{idx}",
                    )
            st.markdown(f"**Signal from users:** {episode.signal_from_users if episode.signal_from_users else '无'}")
            if episode.llm_error:
                st.error("LLM Error")

    st.divider()
    st.markdown("### Response Progress Steps")

    if not detail.response_steps:
        st.info("当前没有 response_progress step judgment。")
    for step in detail.response_steps:
        progress = step.response_progress or "-"
        title = (
            f"Response {step.response_index} | episode={step.episode_index} | "
            f"feedback={step.feedback_kind} | progress: :{_response_color(progress)}[{progress}]"
        )
        with st.expander(title):
            _render_expandable_text("当前 User", step.user_message, 220, f"response_{step.response_index}_user")
            _render_expandable_text("Assistant", step.assistant_message or "", 320, f"response_{step.response_index}_assistant")
            st.markdown(f"**assistant_message_index:** {step.assistant_message_index}")
            if step.tool_calls:
                tool_names = [call.get("name", "unknown") for call in step.tool_calls]
                st.markdown(f"**Tool calls:** {', '.join(tool_names)}")
            st.markdown(
                f"**feedback_range:** {step.feedback_message_start_index if step.feedback_message_start_index is not None else '-'} -> {step.feedback_message_end_index if step.feedback_message_end_index is not None else '-'}"
            )
            st.markdown(f"**Feedback payload:** {step.feedback_payload if step.feedback_payload else '无'}")
            if step.llm_error:
                st.error("LLM Error")

    store.close()

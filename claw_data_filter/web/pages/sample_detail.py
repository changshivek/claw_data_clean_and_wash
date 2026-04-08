"""Sample detail page - shows all turns for a sample."""
import streamlit as st

from claw_data_filter.storage.duckdb_store import DuckDBStore
from claw_data_filter.web.config import DB_PATH
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


def render(route: RouteState):
    st.title("Sample 详情")

    if route.sample_id is None:
        st.error("未指定 sample_id")
        return

    store = DuckDBStore(DB_PATH, read_only=True)

    sample = store.get_sample_by_id(route.sample_id)

    if not sample:
        st.error(f"Sample {route.sample_id} 不存在")
        store.close()
        return

    detail = build_sample_detail_view(sample, store.get_turn_judgments(route.sample_id))

    # Info card
    st.markdown("### 基本信息")
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.markdown(f"**sample_id:** {detail.sample_id}")
    col2.markdown(f"**judged_turns:** {detail.num_turns}")
    col3.markdown(f"**stored_expected_judgments:** {detail.expected_judgment_count}")
    col4.markdown(f"**num_tool_calls:** {detail.num_tool_calls}")
    col5.markdown(f"**helpful_rate:** {detail.helpful_rate:.2f}")
    col6.markdown(f"**satisfied_rate:** {detail.satisfied_rate:.2f}")

    st.caption(f"sample_uid: {detail.sample_uid}")
    st.caption(f"processing_status: {detail.processing_status}")

    st.divider()

    col_back, _ = st.columns([1, 4])
    back_target = route.back_target
    back_label = PAGE_LABELS[back_target]
    if col_back.button(f"← 返回{back_label}"):
        go_back(st.query_params, route)
        st.rerun()

    st.markdown("### Turn 数据")

    # Render turns
    if not detail.turns:
        st.info("没有找到对话记录")
        store.close()
        return

    for turn in detail.turns:
        helpful = turn.response_helpful or "-"
        satisfied = turn.user_satisfied or "-"
        signals = turn.signal_from_users
        llm_error = turn.llm_error

        helpful_color = "green" if helpful == "yes" else ("orange" if helpful == "uncertain" else "red")
        satisfied_color = "green" if satisfied == "yes" else ("gray" if satisfied == "neutral" else "red")

        with st.expander(f"**Turn {turn.turn_index}** | helpful: :{helpful_color}[{helpful}] | satisfied: :{satisfied_color}[{satisfied}]"):
            _render_expandable_text("User", turn.user_message, 200, f"turn_{turn.turn_index}_user")
            _render_expandable_text("Assistant", turn.assistant_message or "", 300, f"turn_{turn.turn_index}_assistant")

            if turn.tool_calls:
                tool_names = [call.get("name", "unknown") for call in turn.tool_calls]
                st.markdown(f"**Tool calls:** {', '.join(tool_names)}")

            if turn.tool_result:
                _render_expandable_text("Tool result", turn.tool_result, 300, f"turn_{turn.turn_index}_tool_result")

            st.markdown(f"**Signal from users:** {signals if signals else '无'}")
            if llm_error:
                st.error("LLM Error")

    store.close()
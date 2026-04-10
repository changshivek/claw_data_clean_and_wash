"""Statistics overview page."""
import streamlit as st

from claw_data_filter.storage.duckdb_store import DuckDBStore
from claw_data_filter.web.components.page_shell import render_page_header
from claw_data_filter.web.config import get_active_db_path
from claw_data_filter.web.services.overview_service import get_processing_status_counts, get_session_merge_counts


def render():
    render_page_header(
        "统计概览",
        "从样本规模、处理状态、session merge 和 empty response 四个层面快速了解当前数据库的整体质量。",
        "Overview",
    )

    store = DuckDBStore(get_active_db_path(st.session_state), read_only=True)
    stats = store.get_stats()
    processed_count = store.get_processed_count()
    status_counts = get_processing_status_counts(store)
    merge_counts = get_session_merge_counts(store)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("总样本数", stats["total_samples"])
    col2.metric("已处理", processed_count)
    col3.metric("平均 Response Progress Rate", f"{stats['avg_response_progress_rate']:.2f}")
    col4.metric("平均 User Satisfied Rate", f"{stats['avg_user_satisfied_rate']:.2f}")

    st.caption("response_progress_rate 基于 assistant response steps 统计；user_satisfied_rate 基于 user episodes 统计。")

    st.divider()

    col5, col6, col7, col8 = st.columns(4)
    col5.metric("平均 Response Regress Rate", f"{stats['avg_response_regress_rate']:.2f}")
    col6.metric("平均 User Negative Feedback Rate", f"{stats['avg_user_negative_feedback_rate']:.2f}")
    col7.metric("错误样本数", stats["error_count"])
    col8.metric("Failed", status_counts["failed"])

    st.divider()

    col9, col10, col11 = st.columns(3)
    col9.metric("Pending", status_counts["pending"])
    col10.metric("Processing", status_counts["processing"])
    col11.metric("Completed", status_counts["completed"])

    st.divider()

    col12, col13, col14, col15, col16 = st.columns(5)
    col12.metric("Merge Keep", merge_counts["keep"])
    col13.metric("Merge Merged", merge_counts["merged"])
    col14.metric("Merge Skipped", merge_counts["skipped"])
    col15.metric("Merge Unmarked", merge_counts["unmarked"])
    col16.metric("Empty Response", merge_counts["empty_response"])

    store.close()

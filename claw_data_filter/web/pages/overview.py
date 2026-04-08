"""Statistics overview page."""
import streamlit as st
from claw_data_filter.storage.duckdb_store import DuckDBStore
from claw_data_filter.web.config import DB_PATH
from claw_data_filter.web.services.overview_service import get_processing_status_counts


def render():
    st.title("统计概览")

    store = DuckDBStore(DB_PATH, read_only=True)
    stats = store.get_stats()
    processed_count = store.get_processed_count()
    status_counts = get_processing_status_counts(store)

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("总样本数", stats["total_samples"])
    col2.metric("已处理", processed_count)
    col3.metric("平均 Helpful Rate", f"{stats['avg_response_helpful_rate']:.2f}")
    col4.metric("平均 Satisfied Rate", f"{stats['avg_user_satisfied_rate']:.2f}")

    st.divider()

    col5, col6, col7, col8 = st.columns(4)
    col5.metric("平均 Unhelpful Rate", f"{stats['avg_response_unhelpful_rate']:.2f}")
    col6.metric("平均负反馈 Rate", f"{stats['avg_user_negative_feedback_rate']:.2f}")
    col7.metric("错误样本数", stats["error_count"])
    col8.metric("Failed", status_counts["failed"])

    st.divider()

    col9, col10, _, _ = st.columns(4)
    col9.metric("Pending", status_counts["pending"])
    col10.metric("Processing", status_counts["processing"])

    store.close()
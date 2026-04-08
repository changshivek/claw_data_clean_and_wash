"""Data filter page."""
from datetime import date
from pathlib import Path

import streamlit as st

from claw_data_filter.exporters.jsonl_exporter import JSONLExporter
from claw_data_filter.storage.duckdb_store import DuckDBStore
from claw_data_filter.web.components.page_shell import render_page_header
from claw_data_filter.web.components.sample_table import render_samples_table
from claw_data_filter.web.config import get_active_db_path
from claw_data_filter.web.services.sample_query_service import get_filtered_samples
from claw_data_filter.web.state.models import RouteState
from claw_data_filter.web.state.router import go_to_detail
from claw_data_filter.web.view_models.filter_list_view import FilterCriteria, load_filter_list_view, save_filter_list_view


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def render(route: RouteState):
    render_page_header(
        "数据筛选",
        "在一页内组合 helpful、满意度、session merge 和 empty response 条件，直接检查样本并按需导出选中记录。",
        "Filter",
    )

    view = load_filter_list_view(st.session_state)
    criteria = view.criteria
    selection_widget_key = "filter.selection_enabled_widget"

    with st.form("filter_form"):
        empty_response_options = ["all", "empty_only", "non_empty_only"]
        merge_scope_options = ["all", "keep", "merged"]
        merge_status_options = ["all", "keep", "merged", "skipped", "unmarked"]

        col1, col2, col3 = st.columns(3)

        helpful_ops = [">=", "<=", "=", "!="]
        helpful_op = col1.selectbox("Helpful Rate", helpful_ops, index=helpful_ops.index(criteria.helpful_op), key="filter.helpful_op")
        helpful_val = col1.number_input("值", min_value=0.0, max_value=1.0, value=float(criteria.helpful_val or 0.0), step=0.1, key="filter.helpful_val")

        satisfied_ops = [">=", "<=", "=", "!="]
        satisfied_op = col2.selectbox("Satisfied Rate", satisfied_ops, index=satisfied_ops.index(criteria.satisfied_op), key="filter.satisfied_op")
        satisfied_val = col2.number_input("值", min_value=0.0, max_value=1.0, value=float(criteria.satisfied_val or 0.0), step=0.1, key="filter.satisfied_val")

        negative_feedback_ops = [">=", "<=", "=", "!="]
        negative_feedback_op = col3.selectbox("Negative Feedback Rate", negative_feedback_ops, index=negative_feedback_ops.index(criteria.negative_feedback_op), key="filter.negative_feedback_op")
        negative_feedback_val = col3.number_input("负反馈值", min_value=0.0, max_value=1.0, value=float(criteria.negative_feedback_val or 0.0), step=0.1, key="filter.negative_feedback_val")

        col4, col5, col6 = st.columns(3)
        num_turns_min = col4.number_input("最小轮次", min_value=0, value=int(criteria.num_turns_min or 0), key="filter.num_turns_min")
        num_turns_max = col5.number_input("最大轮次", min_value=0, value=int(criteria.num_turns_max or 100), key="filter.num_turns_max")
        date_defaults = []
        parsed_date_from = _parse_date(criteria.date_from)
        parsed_date_to = _parse_date(criteria.date_to)
        if parsed_date_from:
            date_defaults.append(parsed_date_from)
        if parsed_date_to:
            date_defaults.append(parsed_date_to)
        date_range = col6.date_input("日期范围", value=date_defaults, key="filter.date_range")

        col7, col8, col9 = st.columns(3)
        empty_response_scope = col7.selectbox(
            "Empty Response",
            empty_response_options,
            index=empty_response_options.index(criteria.empty_response_scope),
            key="filter.empty_response_scope",
            format_func=lambda value: {
                "all": "全部样本",
                "empty_only": "仅 empty response",
                "non_empty_only": "排除 empty response",
            }[value],
        )
        session_merge_scope = col8.selectbox(
            "Session Merge 范围",
            merge_scope_options,
            index=merge_scope_options.index(criteria.session_merge_scope),
            key="filter.session_merge_scope",
            format_func=lambda value: {
                "all": "全部样本",
                "keep": "仅可流转样本",
                "merged": "仅已合并样本",
            }[value],
        )
        session_merge_status = col9.selectbox(
            "Session Merge 状态",
            merge_status_options,
            index=merge_status_options.index(criteria.session_merge_status),
            key="filter.session_merge_status",
            format_func=lambda value: {
                "all": "全部状态",
                "keep": "keep",
                "merged": "merged",
                "skipped": "skipped",
                "unmarked": "未执行",
            }[value],
        )

        col_btn1, col_btn2 = st.columns([1, 1])
        submitted = col_btn1.form_submit_button("应用筛选")
        reset = col_btn2.form_submit_button("重置")

    if submitted:
        date_from_val = str(date_range[0]) if len(date_range) > 0 and date_range[0] else None
        date_to_val = str(date_range[1]) if len(date_range) > 1 and date_range[1] else None
        view.criteria = FilterCriteria(
            helpful_op=helpful_op,
            helpful_val=helpful_val,
            satisfied_op=satisfied_op,
            satisfied_val=satisfied_val,
            negative_feedback_op=negative_feedback_op,
            negative_feedback_val=negative_feedback_val,
            empty_response_scope=empty_response_scope,
            session_merge_scope=session_merge_scope,
            session_merge_status=session_merge_status,
            num_turns_min=num_turns_min,
            num_turns_max=num_turns_max,
            date_from=date_from_val,
            date_to=date_to_val,
        )
        view.page_index = 1
        view.selected_ids = set()
        save_filter_list_view(st.session_state, view)

    if reset:
        view.criteria = FilterCriteria()
        view.page_index = 1
        view.selected_ids = set()
        save_filter_list_view(st.session_state, view)

    if selection_widget_key not in st.session_state:
        st.session_state[selection_widget_key] = view.selection_enabled

    selection_enabled = st.checkbox("选择模式", value=view.selection_enabled, key=selection_widget_key)
    if selection_enabled != view.selection_enabled:
        view.selection_enabled = selection_enabled
        if not selection_enabled:
            view.selected_ids = set()
        save_filter_list_view(st.session_state, view)

    store = DuckDBStore(get_active_db_path(st.session_state), read_only=True)
    with st.spinner("加载数据中..."):
        samples, total = get_filtered_samples(store, view.criteria, view.page_index, view.page_size)

    total_pages = max(1, (total + view.page_size - 1) // view.page_size)
    if view.page_index > total_pages:
        view.page_index = total_pages
        save_filter_list_view(st.session_state, view)
        st.rerun()

    st.divider()
    col_count, col_export = st.columns([3, 1])
    col_count.markdown(f"**共 {total} 条结果**")

    if view.selection_enabled:
        if col_export.button("导出选中"):
            selected_ids = sorted(view.selected_ids)
            if selected_ids:
                with st.spinner("导出中..."):
                    try:
                        placeholders = ", ".join(["?"] * len(selected_ids))
                        output_path = "data/exported_selected.jsonl"
                        exporter = JSONLExporter(store)
                        count = exporter.export(Path(output_path), filter_query=f"id IN ({placeholders})", filter_params=selected_ids)
                        st.success(f"成功导出 {count} 条数据到 {output_path}")
                    except Exception as exc:
                        st.error(f"导出失败: {str(exc)}")
            else:
                st.warning("请先选择要导出的记录")

    def on_detail(sample_id: int) -> None:
        go_to_detail(st.query_params, sample_id, route.active_main_page)
        st.rerun()

    def on_page_change(page_index: int) -> None:
        view.page_index = page_index
        save_filter_list_view(st.session_state, view)
        st.rerun()

    def on_selection_change(selected_ids: set[int]) -> None:
        view.selected_ids = selected_ids
        save_filter_list_view(st.session_state, view)

    render_samples_table(
        samples,
        view.page_index,
        total_pages,
        on_detail,
        on_page_change=on_page_change,
        on_selection_change=on_selection_change,
        selected_ids=view.selected_ids,
        show_checkboxes=view.selection_enabled,
    )

    store.close()

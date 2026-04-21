"""Data filter page."""
from datetime import date
from pathlib import Path

import streamlit as st

from claw_data_filter.exporters.unified_exporter import (
    OPENAI_ROUND_FEEDBACK,
    RAW_JSONL,
    ExportFilterSpec,
    ExportRequest,
    UnifiedExporter,
)
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
        "在一页内组合 response-step progress、episode satisfied、session merge 和 empty response 条件，直接检查样本并按需导出选中记录。",
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

        progress_ops = [">=", "<=", "=", "!="]
        progress_op = col1.selectbox("Response Progress Rate", progress_ops, index=progress_ops.index(criteria.progress_op), key="filter.progress_op")
        progress_val = col1.number_input("值", min_value=0.0, max_value=1.0, value=float(criteria.progress_val or 0.0), step=0.1, key="filter.progress_val")

        satisfied_ops = [">=", "<=", "=", "!="]
        satisfied_op = col2.selectbox("User Satisfied Rate", satisfied_ops, index=satisfied_ops.index(criteria.satisfied_op), key="filter.satisfied_op")
        satisfied_val = col2.number_input("值", min_value=0.0, max_value=1.0, value=float(criteria.satisfied_val or 0.0), step=0.1, key="filter.satisfied_val")

        negative_feedback_ops = [">=", "<=", "=", "!="]
        negative_feedback_op = col3.selectbox("User Negative Feedback Rate", negative_feedback_ops, index=negative_feedback_ops.index(criteria.negative_feedback_op), key="filter.negative_feedback_op")
        negative_feedback_val = col3.number_input("负反馈值", min_value=0.0, max_value=1.0, value=float(criteria.negative_feedback_val or 0.0), step=0.1, key="filter.negative_feedback_val")

        col4, col5, col6 = st.columns(3)
        num_turns_min = col4.number_input("最小 Episode 数", min_value=0, value=int(criteria.num_turns_min or 0), key="filter.num_turns_min")
        num_turns_max = col5.number_input("最大 Episode 数", min_value=0, value=int(criteria.num_turns_max or 100), key="filter.num_turns_max")
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
            progress_op=progress_op,
            progress_val=progress_val,
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
    st.caption("当前 num_turns 筛选字段表示 user episode 数；response_progress_rate 和 user_satisfied_rate 已使用双层 judgment 分母。")
    st.markdown(f"**共 {total} 条结果**")

    export_scope_options = ["filtered"]
    if view.selection_enabled:
        export_scope_options.append("selected")

    with st.expander("导出当前结果", expanded=False):
        col_export1, col_export2, col_export3 = st.columns(3)
        export_scope = col_export1.selectbox(
            "导出范围",
            export_scope_options,
            key="filter.export_scope",
            format_func=lambda value: {
                "filtered": "当前筛选结果",
                "selected": "当前勾选样本",
            }[value],
        )
        export_format = col_export2.selectbox(
            "导出格式",
            [RAW_JSONL, OPENAI_ROUND_FEEDBACK],
            key="filter.export_format",
            format_func=lambda value: {
                RAW_JSONL: "原始 raw_json JSONL",
                OPENAI_ROUND_FEEDBACK: "OpenAI 兼容 + round feedback JSONL",
            }[value],
        )
        default_path = "data/exported.jsonl" if export_format == RAW_JSONL else "data/exported_round_feedback.jsonl"
        output_path = col_export3.text_input("输出文件路径", value=default_path, key="filter.export_output_path")

        if export_scope == "selected":
            st.caption(f"当前已选 {len(view.selected_ids)} 条样本")
        else:
            st.caption(f"当前筛选结果共 {total} 条样本")

        if st.button("开始导出", key="filter.export_button"):
            selected_ids = sorted(view.selected_ids)
            if export_scope == "selected" and not selected_ids:
                st.warning("当前没有勾选样本可导出")
            else:
                filter_spec = _build_export_filter_spec(view.criteria, selected_ids if export_scope == "selected" else None)
                with st.spinner("导出中..."):
                    try:
                        exporter = UnifiedExporter(store)
                        count = exporter.export(
                            ExportRequest(
                                output_path=Path(output_path),
                                export_format=export_format,
                                filter_spec=filter_spec,
                            )
                        )
                        st.success(f"成功导出 {count} 条数据到 {output_path}")
                    except Exception as exc:
                        st.error(f"导出失败: {str(exc)}")

    def on_detail(sample_uid: str) -> None:
        go_to_detail(st.query_params, sample_uid, route.active_main_page)
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


def _build_export_filter_spec(criteria: FilterCriteria, selected_ids: list[int] | None = None) -> ExportFilterSpec:
    empty_response = None
    if criteria.empty_response_scope == "empty_only":
        empty_response = True
    elif criteria.empty_response_scope == "non_empty_only":
        empty_response = False

    session_merge_keep = None
    if criteria.session_merge_scope == "keep":
        session_merge_keep = True
    elif criteria.session_merge_scope == "merged":
        session_merge_keep = False

    return ExportFilterSpec(
        progress_op=criteria.progress_op,
        progress_val=criteria.progress_val,
        satisfied_op=criteria.satisfied_op,
        satisfied_val=criteria.satisfied_val,
        negative_feedback_op=criteria.negative_feedback_op,
        negative_feedback_val=criteria.negative_feedback_val,
        empty_response=empty_response,
        session_merge_keep=session_merge_keep,
        session_merge_status=None if criteria.session_merge_status == "all" else criteria.session_merge_status,
        num_turns_min=criteria.num_turns_min,
        num_turns_max=criteria.num_turns_max,
        date_from=criteria.date_from,
        date_to=criteria.date_to,
        selected_ids=[] if selected_ids is None else selected_ids,
    )

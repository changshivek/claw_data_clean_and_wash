"""Data export page."""
import json
from datetime import date
from pathlib import Path

import streamlit as st

from claw_data_filter.storage.duckdb_store import DuckDBStore
from claw_data_filter.web.components.page_shell import render_page_header
from claw_data_filter.web.config import get_active_db_path
from claw_data_filter.web.services.export_service import fetch_export_rows, preview_export
from claw_data_filter.web.view_models.filter_list_view import FilterCriteria


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _format_size(num_bytes: int) -> str:
    if num_bytes < 1024:
        return f"{num_bytes} B"
    if num_bytes < 1024 * 1024:
        return f"{num_bytes / 1024:.1f} KB"
    return f"{num_bytes / (1024 * 1024):.2f} MB"


def render():
    render_page_header(
        "数据导出",
        "复用与筛选页完全一致的查询语义，先预览数量和大小，再导出 JSONL 或生成统计报告。",
        "Export",
    )

    store = DuckDBStore(get_active_db_path(st.session_state), read_only=True)
    criteria = FilterCriteria()

    with st.form("export_form"):
        empty_response_options = ["all", "empty_only", "non_empty_only"]
        merge_scope_options = ["all", "keep", "merged"]
        merge_status_options = ["all", "keep", "merged", "skipped", "unmarked"]
        col1, col2, col3 = st.columns(3)

        helpful_op = col1.selectbox("Helpful Rate", [">=", "<=", "=", "!="], index=0, key="export.helpful_op")
        helpful_val = col1.number_input("值", min_value=0.0, max_value=1.0, value=0.7, step=0.1, key="export.helpful_val")

        satisfied_op = col2.selectbox("Satisfied Rate", [">=", "<=", "=", "!="], index=0, key="export.satisfied_op")
        satisfied_val = col2.number_input("值", min_value=0.0, max_value=1.0, value=0.5, step=0.1, key="export.satisfied_val")

        negative_feedback_op = col3.selectbox("Negative Feedback Rate", [">=", "<=", "=", "!="], index=0, key="export.negative_feedback_op")
        negative_feedback_val = col3.number_input("负反馈值", min_value=0.0, max_value=1.0, value=0.0, step=0.1, key="export.negative_feedback_val")

        col4, col5, col6 = st.columns(3)
        num_turns_min = col4.number_input("最小轮次", min_value=0, value=0, key="export.num_turns_min")
        num_turns_max = col5.number_input("最大轮次", min_value=0, value=100, key="export.num_turns_max")
        date_defaults = []
        parsed_date_from = _parse_date(criteria.date_from)
        parsed_date_to = _parse_date(criteria.date_to)
        if parsed_date_from:
            date_defaults.append(parsed_date_from)
        if parsed_date_to:
            date_defaults.append(parsed_date_to)
        date_range = col6.date_input("日期范围", value=date_defaults, key="export.date_range")

        col7, col8, col9 = st.columns(3)
        empty_response_scope = col7.selectbox(
            "Empty Response",
            empty_response_options,
            index=0,
            key="export.empty_response_scope",
            format_func=lambda value: {
                "all": "全部样本",
                "empty_only": "仅 empty response",
                "non_empty_only": "排除 empty response",
            }[value],
        )
        session_merge_scope = col8.selectbox(
            "Session Merge 范围",
            merge_scope_options,
            index=0,
            key="export.session_merge_scope",
            format_func=lambda value: {
                "all": "全部样本",
                "keep": "仅可流转样本",
                "merged": "仅已合并样本",
            }[value],
        )
        session_merge_status = col9.selectbox(
            "Session Merge 状态",
            merge_status_options,
            index=0,
            key="export.session_merge_status",
            format_func=lambda value: {
                "all": "全部状态",
                "keep": "keep",
                "merged": "merged",
                "skipped": "skipped",
                "unmarked": "未执行",
            }[value],
        )

        output_path = st.text_input("输出文件路径", value="data/exported.jsonl", key="export.output_path")

        st.markdown("**选择导出字段**")
        col_f1, col_f2 = st.columns(2)
        export_raw_json = col_f1.checkbox("raw_json", value=True, key="export.raw_json")
        export_tool_stats = col_f2.checkbox("tool_stats", value=True, key="export.tool_stats")

        col_btn1, col_btn2 = st.columns(2)
        preview = col_btn1.form_submit_button("预览数量")
        export = col_btn2.form_submit_button("导出")

    date_from = str(date_range[0]) if len(date_range) > 0 and date_range[0] else None
    date_to = str(date_range[1]) if len(date_range) > 1 and date_range[1] else None
    criteria = FilterCriteria(
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
        date_from=date_from,
        date_to=date_to,
    )

    if preview:
        with st.spinner("加载中..."):
            preview_data = preview_export(store, criteria)
            st.info(f"预览: 将导出 {preview_data['count']} 条数据，估算文件大小约 {_format_size(int(preview_data['estimated_bytes']))}")

    if export:
        with st.spinner("导出中..."):
            try:
                columns = ["raw_json"]
                if export_tool_stats:
                    columns.append("tool_stats")
                columns.append("id")

                rows = fetch_export_rows(store, criteria, columns)

                output = Path(output_path)
                output.parent.mkdir(parents=True, exist_ok=True)

                count = 0
                with open(output, "w", encoding="utf-8") as file_handle:
                    for row in rows:
                        data = {}
                        for index, col in enumerate(columns):
                            if col == "raw_json":
                                data[col] = json.loads(row[index]) if row[index] else {}
                            elif col == "tool_stats":
                                data[col] = json.loads(row[index]) if row[index] else {}
                            else:
                                data[col] = row[index]
                        file_handle.write(json.dumps(data, ensure_ascii=False) + "\n")
                        count += 1

                st.success(f"成功导出 {count} 条数据到 {output_path}")
            except Exception as exc:
                st.error(f"导出失败: {str(exc)}")

    store.close()

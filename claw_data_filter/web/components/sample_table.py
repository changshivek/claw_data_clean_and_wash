"""Reusable sample table component."""
import streamlit as st
from typing import Callable


def render_samples_table(
    samples: list[dict],
    page: int,
    total_pages: int,
    on_detail_click: Callable[[str], None],
    on_page_change: Callable[[int], None] | None = None,
    on_selection_change: Callable[[set[int]], None] | None = None,
    selected_ids: set[int] | None = None,
    show_checkboxes: bool = False,
    show_pagination: bool = True,
):
    """Render a paginated sample table.

    Args:
        samples: List of sample dicts with id, num_turns, progress_rate, etc.
        page: Current page number (1-indexed)
        total_pages: Total number of pages
        on_detail_click: Callback(sample_uid) when detail is clicked
        show_checkboxes: If True, show checkboxes for row selection
    """
    if not samples:
        st.info("没有找到匹配的样本")
        return

    current_selected = set(selected_ids or set())
    next_selected = set(current_selected)

    # Table header
    if show_checkboxes:
        header_cols = st.columns([0.5, 0.7, 0.8, 0.9, 1.2, 0.8, 1, 0.8, 0.8, 1])
        header_cols[0].markdown("**选择**")
        header_cols[1].markdown("**ID**")
        header_cols[2].markdown("**episodes**")
        header_cols[3].markdown("**empty**")
        header_cols[4].markdown("**merge**")
        header_cols[5].markdown("**progress**")
        header_cols[6].markdown("**status**")
        header_cols[7].markdown("**satisfied**")
        header_cols[8].markdown("**has_error**")
        header_cols[9].markdown("**操作**")
    else:
        cols = st.columns([0.7, 0.8, 0.9, 1.2, 0.8, 1, 0.8, 0.8, 1])
        headers = ["ID", "episodes", "empty", "merge", "progress_rate", "status", "satisfied_rate", "has_error", "操作"]
        for col, header in zip(cols, headers):
            col.markdown(f"**{header}**")

    # Table rows
    for sample in samples:
        merge_status = sample.get("session_merge_status") or "unmarked"
        merge_reason = sample.get("session_merge_reason")
        merge_text = merge_status if not merge_reason else f"{merge_status}/{merge_reason}"
        empty_response_text = "✓" if sample.get("empty_response") else "-"
        if show_checkboxes:
            cols = st.columns([0.5, 0.7, 0.8, 0.9, 1.2, 0.8, 1, 0.8, 0.8, 1])
            checked = cols[0].checkbox("", key=f"select_{sample['id']}", value=sample["id"] in current_selected)
            if checked:
                next_selected.add(sample["id"])
            else:
                next_selected.discard(sample["id"])
            cols[1].write(sample["id"])
            cols[2].write(sample.get("num_turns", 0))
            cols[3].write(empty_response_text)
            cols[4].write(merge_text)
            cols[5].write(f"{sample.get('progress_rate', 0):.2f}")
            cols[6].write(sample.get("processing_status", "pending"))
            cols[7].write(f"{sample.get('satisfied_rate', 0):.2f}")
            cols[8].write("✓" if sample.get("has_error") else "-")
            if cols[9].button("详情", key=f"detail_{sample['sample_uid']}"):
                on_detail_click(sample["sample_uid"])
        else:
            cols = st.columns([0.7, 0.8, 0.9, 1.2, 0.8, 1, 0.8, 0.8, 1])
            cols[0].write(sample["id"])
            cols[1].write(sample.get("num_turns", 0))
            cols[2].write(empty_response_text)
            cols[3].write(merge_text)
            cols[4].write(f"{sample.get('progress_rate', 0):.2f}")
            cols[5].write(sample.get("processing_status", "pending"))
            cols[6].write(f"{sample.get('satisfied_rate', 0):.2f}")
            cols[7].write("✓" if sample.get("has_error") else "-")
            if cols[8].button("详情", key=f"detail_{sample['sample_uid']}"):
                on_detail_click(sample["sample_uid"])

    if on_selection_change and next_selected != current_selected:
        on_selection_change(next_selected)

    if show_pagination:
        col_prev, col_page, col_next = st.columns([1, 2, 1])
        if page > 1 and col_prev.button("上一页"):
            if on_page_change:
                on_page_change(page - 1)
        col_page.markdown(f"第 {page} / {total_pages} 页")
        if page < total_pages and col_next.button("下一页"):
            if on_page_change:
                on_page_change(page + 1)

    # Show selected count if checkboxes are shown
    if show_checkboxes and next_selected:
        st.info(f"已选择 {len(next_selected)} 条记录")

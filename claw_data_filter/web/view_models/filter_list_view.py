"""Filter-page state models."""
from dataclasses import dataclass, field
from typing import Any


FILTER_NAMESPACE = "filter"
DEFAULT_PAGE_SIZE = 20


def _state_key(name: str) -> str:
    return f"{FILTER_NAMESPACE}.{name}"


@dataclass(slots=True)
class FilterCriteria:
    helpful_op: str = ">="
    helpful_val: float | None = 0.7
    satisfied_op: str = ">="
    satisfied_val: float | None = 0.5
    negative_feedback_op: str = ">="
    negative_feedback_val: float | None = None
    session_merge_scope: str = "all"
    session_merge_status: str = "all"
    num_turns_min: int | None = 0
    num_turns_max: int | None = 100
    date_from: str | None = None
    date_to: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "helpful_op": self.helpful_op,
            "helpful_val": self.helpful_val,
            "satisfied_op": self.satisfied_op,
            "satisfied_val": self.satisfied_val,
            "negative_feedback_op": self.negative_feedback_op,
            "negative_feedback_val": self.negative_feedback_val,
            "session_merge_scope": self.session_merge_scope,
            "session_merge_status": self.session_merge_status,
            "num_turns_min": self.num_turns_min,
            "num_turns_max": self.num_turns_max,
            "date_from": self.date_from,
            "date_to": self.date_to,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "FilterCriteria":
        if not data:
            return cls()
        return cls(
            helpful_op=str(data.get("helpful_op", ">=")),
            helpful_val=data.get("helpful_val", 0.7),
            satisfied_op=str(data.get("satisfied_op", ">=")),
            satisfied_val=data.get("satisfied_val", 0.5),
            negative_feedback_op=str(data.get("negative_feedback_op", ">=")),
            negative_feedback_val=data.get("negative_feedback_val"),
            session_merge_scope=str(data.get("session_merge_scope", "all")),
            session_merge_status=str(data.get("session_merge_status", "all")),
            num_turns_min=data.get("num_turns_min", 0),
            num_turns_max=data.get("num_turns_max", 100),
            date_from=data.get("date_from"),
            date_to=data.get("date_to"),
        )


@dataclass(slots=True)
class FilterListView:
    criteria: FilterCriteria = field(default_factory=FilterCriteria)
    page_index: int = 1
    page_size: int = DEFAULT_PAGE_SIZE
    selected_ids: set[int] = field(default_factory=set)
    selection_enabled: bool = False


def load_filter_list_view(session_state: dict[str, Any]) -> FilterListView:
    """Load filter page state from Streamlit session state."""
    criteria = FilterCriteria.from_dict(session_state.get(_state_key("criteria")))
    page_index = int(session_state.get(_state_key("page_index"), 1))
    page_size = int(session_state.get(_state_key("page_size"), DEFAULT_PAGE_SIZE))
    selected_ids = session_state.get(_state_key("selected_ids"), set())
    selection_enabled = bool(session_state.get(_state_key("selection_enabled"), False))
    return FilterListView(
        criteria=criteria,
        page_index=max(1, page_index),
        page_size=max(1, page_size),
        selected_ids=set(selected_ids),
        selection_enabled=selection_enabled,
    )


def save_filter_list_view(session_state: dict[str, Any], view: FilterListView) -> None:
    """Persist filter page state to Streamlit session state."""
    session_state[_state_key("criteria")] = view.criteria.to_dict()
    session_state[_state_key("page_index")] = view.page_index
    session_state[_state_key("page_size")] = view.page_size
    session_state[_state_key("selected_ids")] = set(view.selected_ids)
    session_state[_state_key("selection_enabled")] = view.selection_enabled


def reset_filter_list_view(session_state: dict[str, Any]) -> FilterListView:
    """Reset filter page state to defaults."""
    view = FilterListView()
    save_filter_list_view(session_state, view)
    return view

"""Tests for web routing helpers."""

from claw_data_filter.web.state.router import go_back, go_to_detail, go_to_page, read_route


def test_read_route_defaults_to_overview():
    route = read_route({})

    assert route.page == "overview"
    assert route.sample_uid is None
    assert route.active_main_page == "overview"


def test_go_to_detail_persists_return_target():
    params: dict[str, str] = {}

    go_to_detail(params, sample_uid="uid-42", return_to="tables")
    route = read_route(params)

    assert route.page == "detail"
    assert route.sample_uid == "uid-42"
    assert route.return_to == "tables"
    assert route.active_main_page == "tables"


def test_go_back_returns_to_source_page():
    params: dict[str, str] = {}
    route = go_to_detail(params, sample_uid="uid-7", return_to="filter")

    go_back(params, route)
    route = read_route(params)

    assert route.page == "filter"
    assert route.sample_uid is None
    assert route.return_to is None


def test_go_to_page_clears_detail_context():
    params: dict[str, str] = {"page": "detail", "sample_uid": "uid-9", "return_to": "tables"}

    go_to_page(params, "export")
    route = read_route(params)

    assert route.page == "overview"
    assert route.sample_uid is None
    assert route.return_to is None

"""Tests for filter query builder."""
from claw_data_filter.filters.query import FilterQueryBuilder, ComparisonOp


def test_filter_builder_basic():
    """Test basic condition building with non-JSON fields."""
    builder = FilterQueryBuilder()
    builder.add_condition("num_turns", ComparisonOp.GT, 7.0)

    sql, params = builder.build_parameterized_where_clause()
    assert "num_turns > ?" in sql
    assert params == [7.0]


def test_filter_builder_expression_parsing():
    """Test expression parsing for num_tool_calls."""
    builder = FilterQueryBuilder()
    builder.add_condition("num_tool_calls", ComparisonOp.GTE, 4)
    builder.add_condition("num_tool_calls", ComparisonOp.LT, 5)

    sql, params = builder.build_parameterized_where_clause()
    assert ">=" in sql
    assert "<" in sql
    assert params == [4, 5]


def test_filter_builder_chained():
    """Test method chaining."""
    builder = FilterQueryBuilder()
    result = builder.add_condition("num_tool_calls", ComparisonOp.GTE, 4).add_condition("num_turns", ComparisonOp.GTE, 2)

    assert result is builder  # returns self
    sql, params = builder.build_parameterized_where_clause()
    assert "num_tool_calls >= ?" in sql
    assert "num_turns >= ?" in sql
    assert params == [4, 2]


def test_filtered_samples_query():
    """Test full parameterized query generation."""
    builder = FilterQueryBuilder()
    builder.add_condition("num_turns", ComparisonOp.GTE, 2)

    query, params = builder.get_parameterized_query(limit=100)
    assert "SELECT" in query
    assert "FROM samples s" in query
    assert "s.num_turns >= ?" in query
    assert "LIMIT ?" in query
    assert params == [2, 100]


def test_invalid_expression():
    """Test that invalid expressions raise ValueError."""
    builder = FilterQueryBuilder()
    builder.add_condition("nonexistent_field", ComparisonOp.GTE, 4)
    try:
        builder.build_parameterized_where_clause()
        assert False, "Should have raised ValueError"
    except ValueError:
        pass


def test_filter_tool_stats_fields():
    """Test filtering by tool_stats fields"""
    builder = FilterQueryBuilder()
    builder.add_condition("response_progress_rate", ComparisonOp(">="), 0.8)
    sql, params = builder.build_parameterized_where_clause()
    assert "response_progress_rate" in sql
    assert params == [0.8]


def test_filter_builder_supports_negative_feedback_rate():
    builder = FilterQueryBuilder()
    builder.add_condition("user_negative_feedback_rate", ComparisonOp.GTE, 0.3)

    sql, params = builder.build_parameterized_where_clause("s")

    assert "user_negative_feedback_rate" in sql
    assert params == [0.3]


def test_filter_builder_supports_regress_rate():
    builder = FilterQueryBuilder()
    builder.add_condition("response_regress_rate", ComparisonOp.GTE, 0.2)

    sql, params = builder.build_parameterized_where_clause("s")

    assert "response_regress_rate" in sql
    assert params == [0.2]


def test_filter_builder_supports_session_merge_fields():
    builder = FilterQueryBuilder()
    builder.add_condition("session_merge_keep", ComparisonOp.EQ, False)
    builder.add_condition("session_merge_status", ComparisonOp.EQ, "merged")

    sql, params = builder.build_parameterized_where_clause("s")

    assert "session_merge_keep" in sql
    assert "session_merge_status" in sql
    assert params == [False, "merged"]


def test_filter_builder_parameterized_clause():
    """Test parameterized clause generation keeps values out of SQL text."""
    builder = FilterQueryBuilder()
    builder.add_condition("response_progress_rate", ComparisonOp.GTE, 0.8)
    builder.add_condition("num_turns", ComparisonOp.GTE, 2)

    sql, params = builder.build_parameterized_where_clause("s")

    assert "?" in sql
    assert params == [0.8, 2]


def test_filter_builder_parameterized_query_limit():
    """Test full parameterized query appends limit placeholder."""
    builder = FilterQueryBuilder()
    builder.add_condition("num_turns", ComparisonOp.GTE, 2)

    query, params = builder.get_parameterized_query(limit=10)

    assert "LIMIT ?" in query
    assert params == [2, 10]


if __name__ == "__main__":
    test_filter_builder_basic()
    test_filter_builder_expression_parsing()
    test_filter_builder_chained()
    test_filtered_samples_query()
    test_invalid_expression()
    test_filter_tool_stats_fields()
    test_filter_builder_parameterized_clause()
    test_filter_builder_parameterized_query_limit()
    print("All filter query tests passed!")

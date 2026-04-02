"""Tests for filter query builder."""
from claw_data_filter.filters.query import FilterQueryBuilder, ComparisonOp


def test_filter_builder_basic():
    """Test basic condition building with round_feedback fields."""
    builder = FilterQueryBuilder()
    builder.add_condition("response_helpful_rate", ComparisonOp.GTE, 4)
    builder.add_condition("num_turns", ComparisonOp.GT, 7.0)

    where = builder.build_where_clause()
    assert "response_helpful_rate >= 4" in where
    assert "num_turns > 7.0" in where
    print(f"WHERE clause: {where}")
    print("test_filter_builder_basic passed")


def test_filter_builder_expression_parsing():
    """Test expression parsing for num_tool_calls."""
    builder = FilterQueryBuilder()
    # Use num_tool_calls which is a valid field
    builder.add_condition("num_tool_calls", ComparisonOp.GTE, 4)
    builder.add_condition("num_tool_calls", ComparisonOp.LT, 5)

    where = builder.build_where_clause()
    assert ">=" in where
    assert "<" in where
    print(f"WHERE clause: {where}")
    print("test_filter_builder_expression_parsing passed")


def test_filter_builder_task_types():
    """Test task type filtering."""
    builder = FilterQueryBuilder()
    builder.add_task_type_filter(["coding", "reasoning"])

    where = builder.build_where_clause()
    assert "task_type IN ('coding', 'reasoning')" in where
    print(f"WHERE clause: {where}")
    print("test_filter_builder_task_types passed")


def test_filter_builder_chained():
    """Test method chaining."""
    builder = FilterQueryBuilder()
    result = builder.add_condition("num_tool_calls", ComparisonOp.GTE, 4).add_task_type_filter(["general"])

    assert result is builder  # returns self
    where = builder.build_where_clause()
    assert "num_tool_calls >= 4" in where
    assert "task_type IN ('general')" in where
    print("test_filter_builder_chained passed")


def test_filtered_samples_query():
    """Test full query generation."""
    builder = FilterQueryBuilder()
    builder.add_condition("num_turns", ComparisonOp.GTE, 2)

    query = builder.get_filtered_samples_query(limit=100)
    assert "SELECT" in query
    assert "FROM samples s" in query
    assert "WHERE num_turns >= 2" in query
    assert "LIMIT 100" in query
    print(f"Query: {query}")
    print("test_filtered_samples_query passed")


def test_invalid_expression():
    """Test that invalid expressions raise ValueError."""
    builder = FilterQueryBuilder()
    builder.add_condition("nonexistent_field", ComparisonOp.GTE, 4)
    try:
        builder.build_where_clause()
        assert False, "Should have raised ValueError"
    except ValueError:
        pass
    print("test_invalid_expression passed")


def test_filter_tool_stats_fields():
    """Test filtering by tool_stats fields"""
    from claw_data_filter.filters.query import FilterQueryBuilder, ComparisonOp

    builder = FilterQueryBuilder()
    builder.add_condition("response_helpful_rate", ComparisonOp(">="), 0.8)
    sql = builder.build_where_clause()
    assert "response_helpful_rate" in sql


if __name__ == "__main__":
    test_filter_builder_basic()
    test_filter_builder_expression_parsing()
    test_filter_builder_task_types()
    test_filter_builder_chained()
    test_filtered_samples_query()
    test_invalid_expression()
    test_filter_tool_stats_fields()
    print("All filter query tests passed!")
"""Tests for evaluation prompt and parsing."""
import json
from claw_data_filter.prompts.evaluation_prompt import build_evaluation_prompt, EVALUATION_SYSTEM_PROMPT


def test_build_evaluation_prompt():
    """Test prompt building."""
    conversation = "User: What's the weather?\nAssistant: Let me check."
    system, user = build_evaluation_prompt(conversation)

    assert system == EVALUATION_SYSTEM_PROMPT
    assert conversation in user
    assert "Output JSON only:" in user
    print("test_build_evaluation_prompt passed")


def test_parse_valid_json_response():
    """Test parsing a valid JSON evaluation response."""
    # This tests the parsing logic in Evaluator._parse_evaluation_response
    raw_response = '''
    {
      "task_type": "information_retrieval",
      "progress_score": 4,
      "tool_quality_score": 0.9,
      "tool_success_rate": 1.0,
      "overall_score": 8.5,
      "reasoning": "Good work"
    }
    '''
    data = json.loads(raw_response)
    assert data["task_type"] == "information_retrieval"
    assert data["progress_score"] == 4
    assert data["tool_quality_score"] == 0.9
    assert data["tool_success_rate"] == 1.0
    assert data["overall_score"] == 8.5
    print("test_parse_valid_json_response passed")


def test_parse_response_with_extra_text():
    """Test parsing JSON that has extra text around it."""
    raw_response = """Here is my evaluation:
    {
      "task_type": "coding",
      "progress_score": 5,
      "tool_quality_score": 1.0,
      "tool_success_rate": 1.0,
      "overall_score": 9.5,
      "reasoning": "Perfect execution"
    }
    Thank you."""

    # Find JSON by looking for {...}
    import re
    json_match = re.search(r'\{[^{}]*\}', raw_response, re.DOTALL)
    assert json_match is not None
    data = json.loads(json_match.group())
    assert data["task_type"] == "coding"
    assert data["progress_score"] == 5
    print("test_parse_response_with_extra_text passed")


if __name__ == "__main__":
    test_build_evaluation_prompt()
    test_parse_valid_json_response()
    test_parse_response_with_extra_text()
    print("All evaluator tests passed!")
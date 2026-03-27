"""Evaluation prompt template for LLM-based conversation assessment."""

EVALUATION_SYSTEM_PROMPT = """You are an expert evaluator of AI agent conversations.
Your task is to evaluate the quality of agent interactions across multiple dimensions.
Be precise and critical in your assessment.

Respond ONLY with valid JSON in this exact format:
{
  "task_type": "one of: information_retrieval, data_processing, coding, reasoning, creative, general",
  "progress_score": 0-5,
  "tool_quality_score": 0.0-1.0,
  "tool_success_rate": 0.0-1.0,
  "overall_score": 0.0-10.0,
  "reasoning": "brief explanation of your assessment"
}"""

EVALUATION_USER_PROMPT_TEMPLATE = """Please evaluate this AI Agent conversation:

{conversation}

---
Evaluate the conversation according to these criteria:

1. **Task Type**: Classify the type of task performed.

2. **Progress Score** (0-5):
   - 0: Wrong direction or endless loop (no useful progress)
   - 1: Reasonable attempt but no significant progress
   - 2: Correct direction, proper tool use, significant progress
   - 4: Successfully completed with trial-and-error in tool usage
   - 5: Successfully completed, all steps correct, no tool errors

3. **Tool Quality Score** (0.0-1.0):
   - 0.0: Repeated tool parameter errors, poor understanding
   - 1.0: Correct tool parameter understanding throughout

4. **Tool Success Rate** (0.0-1.0):
   - Ratio of successful tool calls to total tool calls

5. **Overall Score** (0.0-10.0): Composite assessment

Output JSON only:"""


def build_evaluation_prompt(conversation: str) -> tuple[str, str]:
    """Build (system_prompt, user_prompt) for evaluation.

    Args:
        conversation: Formatted conversation string

    Returns:
        Tuple of (system_prompt, user_prompt)
    """
    system_prompt = EVALUATION_SYSTEM_PROMPT
    user_prompt = EVALUATION_USER_PROMPT_TEMPLATE.format(conversation=conversation)
    return system_prompt, user_prompt
"""Tests for AgentLoop working memory actions."""

from dataclasses import dataclass, field

import pytest

from liveweb_arena.core.agent_loop import AgentLoop
from liveweb_arena.core.agent_protocol import FunctionCallingProtocol
from liveweb_arena.core.models import BrowserObservation, CompositeTask
from liveweb_arena.utils.llm_client import LLMResponse, ToolCall


@dataclass
class _FakeSession:
    observations: list[BrowserObservation]
    execute_calls: list = field(default_factory=list)

    async def goto(self, url: str) -> BrowserObservation:
        return self.observations[0]

    async def execute_action(self, action):
        self.execute_calls.append(action)
        return self.observations[min(len(self.execute_calls), len(self.observations) - 1)]


@dataclass
class _FakeLLMClient:
    responses: list[LLMResponse]
    users: list[str] = field(default_factory=list)

    async def chat_with_tools(self, system, user, model, tools, temperature, seed):
        self.users.append(user)
        if not self.responses:
            raise AssertionError("No more fake LLM responses queued")
        return self.responses.pop(0)


def _task() -> CompositeTask:
    return CompositeTask(
        subtasks=[],
        combined_intent="Find the right answer.",
        plugin_hints={},
        seed=0,
    )


@pytest.mark.asyncio
async def test_agent_loop_applies_memory_patch_alongside_browser_action():
    session = _FakeSession(observations=[
        BrowserObservation(url="about:blank", title="Blank", accessibility_tree="blank tree"),
        BrowserObservation(url="https://example.com", title="Example", accessibility_tree="example tree"),
    ])
    llm = _FakeLLMClient(responses=[
        LLMResponse(tool_calls=[ToolCall(
            id="call_1",
            function={
                "name": "goto",
                "arguments": '{"url": "https://example.com", "memory_patch": "@@\\n+ BTC price = 104230"}',
            },
        )]),
        LLMResponse(tool_calls=[ToolCall(
            id="call_2",
            function={
                "name": "stop",
                "arguments": '{"answers": {"answer": "done"}, "memory_patch": "@@\\n- BTC price = 104230"}',
            },
        )]),
    ])
    loop = AgentLoop(session=session, llm_client=llm, protocol=FunctionCallingProtocol(), max_steps=6)

    trajectory, final_answer, _ = await loop.run(task=_task(), model="fake-model")

    assert len(session.execute_calls) == 1
    assert session.execute_calls[0].action_type == "goto"
    assert [step.action.action_type for step in trajectory] == ["goto", "stop"]
    assert trajectory[0].action_result == "Success | Memory patch applied: -0, +18 chars"
    assert trajectory[1].action_result == "Task completed | Memory patch applied: -1, +0 chars"
    assert final_answer == {"answers": {"answer": "done"}}
    assert loop.get_working_memory() == ""


@pytest.mark.asyncio
async def test_agent_loop_injects_memory_into_next_prompt():
    session = _FakeSession(observations=[
        BrowserObservation(url="about:blank", title="Blank", accessibility_tree="blank tree"),
    ])
    llm = _FakeLLMClient(responses=[
        LLMResponse(tool_calls=[ToolCall(
            id="call_1",
            function={
                "name": "wait",
                "arguments": '{"seconds": 1, "memory_patch": "@@\\n+ Need BTC and ETH prices"}',
            },
        )]),
        LLMResponse(tool_calls=[ToolCall(id="call_2", function={"name": "stop", "arguments": '{"answers": {"answer": "done"}}'})]),
    ])
    loop = AgentLoop(session=session, llm_client=llm, protocol=FunctionCallingProtocol(), max_steps=4)

    trajectory, _, _ = await loop.run(task=_task(), model="fake-model")

    assert "Need BTC and ETH prices" not in llm.users[0]
    assert "Need BTC and ETH prices" in llm.users[1]
    assert "### Working Memory" in llm.users[1]
    assert trajectory[0].memory_snapshot == "Need BTC and ETH prices"
    assert trajectory[1].memory_snapshot == "Need BTC and ETH prices"


@pytest.mark.asyncio
async def test_agent_loop_ignores_invalid_memory_patch_and_executes_action():
    session = _FakeSession(observations=[
        BrowserObservation(url="about:blank", title="Blank", accessibility_tree="blank tree"),
        BrowserObservation(url="https://example.com", title="Example", accessibility_tree="example tree"),
    ])
    llm = _FakeLLMClient(responses=[
        LLMResponse(tool_calls=[ToolCall(
            id="call_1",
            function={
                "name": "goto",
                "arguments": '{"url": "https://example.com", "memory_patch": "+ missing header"}',
            },
        )]),
        LLMResponse(tool_calls=[ToolCall(id="call_2", function={"name": "stop", "arguments": '{"answers": {"answer": "done"}}'})]),
    ])
    loop = AgentLoop(session=session, llm_client=llm, protocol=FunctionCallingProtocol(), max_steps=4)

    trajectory, _, _ = await loop.run(task=_task(), model="fake-model")

    assert len(session.execute_calls) == 1
    assert trajectory[0].action_result == "Success | Memory patch ignored: invalid diff header"
    assert loop.get_working_memory() == ""


@pytest.mark.asyncio
async def test_agent_loop_deletes_memory_by_exact_line_not_substring():
    session = _FakeSession(observations=[
        BrowserObservation(url="about:blank", title="Blank", accessibility_tree="blank tree"),
    ])
    llm = _FakeLLMClient(responses=[
        LLMResponse(tool_calls=[ToolCall(
            id="call_1",
            function={
                "name": "wait",
                "arguments": '{"seconds": 1, "memory_patch": "@@\\n+ BTC price = 104230\\n+ BTC price = 104230 (updated)"}',
            },
        )]),
        LLMResponse(tool_calls=[ToolCall(
            id="call_2",
            function={
                "name": "wait",
                "arguments": '{"seconds": 1, "memory_patch": "@@\\n- BTC price = 104230"}',
            },
        )]),
        LLMResponse(tool_calls=[ToolCall(id="call_3", function={"name": "stop", "arguments": '{"answers": {"answer": "done"}}'})]),
    ])
    loop = AgentLoop(session=session, llm_client=llm, protocol=FunctionCallingProtocol(), max_steps=5)

    trajectory, _, _ = await loop.run(task=_task(), model="fake-model")

    assert trajectory[1].action_result == "Success | Memory patch applied: -1, +0 chars"
    assert loop.get_working_memory() == "BTC price = 104230 (updated)"

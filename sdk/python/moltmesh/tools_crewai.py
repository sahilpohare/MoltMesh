"""
moltmesh.tools_crewai — CrewAI BaseTool wrappers for the p2p-a2a daemon.

Install extras:  pip install "moltmesh[crewai]"

Usage:
    from moltmesh import A2AClient
    from moltmesh.tools_crewai import (
        SendMessageTool,
        CreateTaskTool,
        GetTaskTool,
        FindAgentsTool,
        GetInboxTool,
    )

    client = A2AClient().connect()

    agent = Agent(
        role="Coordinator",
        goal="Delegate tasks to specialist agents via p2p-a2a",
        tools=[
            SendMessageTool(client=client),
            CreateTaskTool(client=client),
            GetTaskTool(client=client),
            FindAgentsTool(client=client),
            GetInboxTool(client=client),
        ],
    )
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Type

from pydantic import BaseModel, Field

try:
    from crewai.tools import BaseTool
except ImportError as e:
    raise ImportError(
        "crewai is required for this module. Install with: pip install 'moltmesh[crewai]'"
    ) from e

if TYPE_CHECKING:
    from moltmesh.client import A2AClient


# ── input schemas ─────────────────────────────────────────────────────────────

class SendMessageInput(BaseModel):
    to_did: str = Field(description="DID of the recipient agent (e.g. did:key:z6Mk...)")
    text: str = Field(description="Plain text content of the message")
    thread_id: str = Field(default="", description="Optional thread ID to attach the message to")


class CreateTaskInput(BaseModel):
    to_did: str = Field(description="DID of the agent that will execute the task")
    skill: str = Field(description="Capability ID the task requires (e.g. a2a:v1:cap:text-generation)")
    thread_id: str = Field(default="", description="Optional thread ID")
    metadata: dict[str, str] = Field(default_factory=dict, description="Optional key-value metadata")


class GetTaskInput(BaseModel):
    task_id: str = Field(description="ID of the task to retrieve")


class CancelTaskInput(BaseModel):
    task_id: str = Field(description="ID of the task to cancel")


class FindAgentsInput(BaseModel):
    capability: str = Field(description="Capability ID to search for (e.g. a2a:v1:cap:image-generation)")
    limit: int = Field(default=5, description="Maximum number of agents to return")


class GetInboxInput(BaseModel):
    thread_id: str = Field(default="", description="Filter by thread ID")
    task_id: str = Field(default="", description="Filter by task ID")
    unread_only: bool = Field(default=False, description="Return only unread messages")
    limit: int = Field(default=20, description="Maximum number of messages to return")


# ── tools ─────────────────────────────────────────────────────────────────────

class SendMessageTool(BaseTool):
    name: str = "p2p_send_message"
    description: str = (
        "Send a text message to another agent identified by their DID. "
        "The message is queued in the outbox and delivered when the peer is reachable."
    )
    args_schema: Type[BaseModel] = SendMessageInput
    client: Any  # A2AClient — Any to avoid pydantic import issues

    class Config:
        arbitrary_types_allowed = True

    def _run(self, to_did: str, text: str, thread_id: str = "") -> str:
        result = self.client.send_message(to_did, text=text, thread_id=thread_id)
        queued = " (queued — peer offline)" if result.queued else ""
        return f"Message sent. ID: {result.message_id}{queued}"


class CreateTaskTool(BaseTool):
    name: str = "p2p_create_task"
    description: str = (
        "Delegate a task to another agent via p2p-a2a. "
        "Specify the assignee DID and the capability (skill) required. "
        "Returns the task ID and initial status."
    )
    args_schema: Type[BaseModel] = CreateTaskInput
    client: Any

    class Config:
        arbitrary_types_allowed = True

    def _run(
        self,
        to_did: str,
        skill: str,
        thread_id: str = "",
        metadata: dict[str, str] | None = None,
    ) -> str:
        task = self.client.create_task(
            to_did,
            skill,
            thread_id=thread_id,
            metadata=metadata or {},
        )
        return (
            f"Task created.\n"
            f"  ID:       {task.id}\n"
            f"  Assignee: {task.assignee}\n"
            f"  Skill:    {task.skill}\n"
            f"  Status:   {task.status}"
        )


class GetTaskTool(BaseTool):
    name: str = "p2p_get_task"
    description: str = (
        "Retrieve the current status and details of a task by its ID. "
        "Use this to poll task progress after creating a task."
    )
    args_schema: Type[BaseModel] = GetTaskInput
    client: Any

    class Config:
        arbitrary_types_allowed = True

    def _run(self, task_id: str) -> str:
        task = self.client.get_task(task_id)
        lines = [
            f"Task {task.id}",
            f"  Status:   {task.status}",
            f"  Skill:    {task.skill}",
            f"  Assignee: {task.assignee}",
        ]
        if task.error:
            lines.append(f"  Error:    {task.error}")
        if task.output_artifacts:
            for a in task.output_artifacts:
                lines.append(f"  Artifact: {a.cid} ({a.mime_type}, {a.size} bytes)")
        return "\n".join(lines)


class CancelTaskTool(BaseTool):
    name: str = "p2p_cancel_task"
    description: str = "Cancel a task that is pending or in progress."
    args_schema: Type[BaseModel] = CancelTaskInput
    client: Any

    class Config:
        arbitrary_types_allowed = True

    def _run(self, task_id: str) -> str:
        task = self.client.cancel_task(task_id)
        return f"Task {task.id} cancelled (status: {task.status})"


class FindAgentsTool(BaseTool):
    name: str = "p2p_find_agents"
    description: str = (
        "Search the p2p network for agents that advertise a given capability. "
        "Returns a list of matching agent DIDs and names."
    )
    args_schema: Type[BaseModel] = FindAgentsInput
    client: Any

    class Config:
        arbitrary_types_allowed = True

    def _run(self, capability: str, limit: int = 5) -> str:
        cards = self.client.find_agents(capability, limit=limit)
        if not cards:
            return f"No agents found for capability: {capability}"
        lines = [f"Found {len(cards)} agent(s) for '{capability}':"]
        for card in cards:
            lines.append(f"  - {card.did}  ({card.name or 'unnamed'})")
        return "\n".join(lines)


class GetInboxTool(BaseTool):
    name: str = "p2p_get_inbox"
    description: str = (
        "Retrieve messages from this agent's inbox. "
        "Optionally filter by thread, task, or unread status."
    )
    args_schema: Type[BaseModel] = GetInboxInput
    client: Any

    class Config:
        arbitrary_types_allowed = True

    def _run(
        self,
        thread_id: str = "",
        task_id: str = "",
        unread_only: bool = False,
        limit: int = 20,
    ) -> str:
        msgs = self.client.get_inbox(
            thread_id=thread_id,
            task_id=task_id,
            unread_only=unread_only,
            limit=limit,
        )
        if not msgs:
            return "Inbox is empty."
        lines = [f"{len(msgs)} message(s):"]
        for m in msgs:
            lines.append(f"  [{m.id[:8]}] from={m.from_did}  kind={m.kind}")
        return "\n".join(lines)

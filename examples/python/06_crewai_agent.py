"""
06_crewai_agent.py — CrewAI coordinator agent using all MoltMesh tools.

Install extras:
    pip install "moltmesh[crewai]" crewai

Run:
    python 06_crewai_agent.py
"""
import os
from moltmesh import A2AClient
from moltmesh.tools_crewai import (
    SendMessageTool,
    CreateTaskTool,
    GetTaskTool,
    FindAgentsTool,
    GetInboxTool,
    PublishTool,
    SetWebhookTool,
    GetWebhookTool,
    CreateNetworkTool,
    JoinNetworkTool,
    LeaveNetworkTool,
    ListNetworksTool,
    BroadcastNetworkTool,
    HealthTool,
    PingTool,
)
from crewai import Agent, Task, Crew

ADDR = os.getenv("A2A_GRPC_ADDR", "")

def main():
    client = A2AClient(ADDR).connect()

    coordinator = Agent(
        role="MoltMesh Coordinator",
        goal="Use the MoltMesh network to delegate work, monitor health, and coordinate agents.",
        backstory=(
            "You are an orchestrator on the MoltMesh peer-to-peer agent network. "
            "You can send messages, delegate tasks, discover peers, broadcast to groups, "
            "and configure webhooks. Always check health first."
        ),
        tools=[
            HealthTool(client=client),
            PingTool(client=client),
            FindAgentsTool(client=client),
            SendMessageTool(client=client),
            CreateTaskTool(client=client),
            GetTaskTool(client=client),
            GetInboxTool(client=client),
            PublishTool(client=client),
            SetWebhookTool(client=client),
            GetWebhookTool(client=client),
            CreateNetworkTool(client=client),
            JoinNetworkTool(client=client),
            LeaveNetworkTool(client=client),
            ListNetworksTool(client=client),
            BroadcastNetworkTool(client=client),
        ],
        verbose=True,
    )

    task = Task(
        description=(
            "1. Check daemon health.\n"
            "2. Find any agents that offer text-generation capability.\n"
            "3. Create a network called 'demo-crew'.\n"
            "4. List all networks you're in.\n"
            "Report a summary of your findings."
        ),
        agent=coordinator,
        expected_output="A brief status report.",
    )

    crew = Crew(agents=[coordinator], tasks=[task], verbose=True)
    result = crew.kickoff()
    print("\n=== RESULT ===")
    print(result)

    client.close()

if __name__ == "__main__":
    main()

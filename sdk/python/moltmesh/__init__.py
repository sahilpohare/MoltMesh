from moltmesh.client import A2AClient
from moltmesh.capability import (
    CapabilityId,
    CapabilityTag,
    CoreCapability,
    CORE_CAPABILITY_PREFIX,
    capability_id,
    capability_name,
    is_core_capability,
    normalize_capability,
)
from moltmesh.did import normalize_did, short_did
from moltmesh.proto import a2a_pb2 as pb

# Re-export proto types for convenient access
HealthResponse = pb.HealthResponse
PeerInfo = pb.PeerInfo
PingResponse = pb.PingResponse
TopicMessage = pb.TopicMessage
NetworkInfo = pb.NetworkInfo
NetworkMember = pb.NetworkMember
BroadcastMessage = pb.BroadcastMessage

# Task status constants at package level
STATUS_SUBMITTED = pb.TASK_STATUS_SUBMITTED
STATUS_WORKING   = pb.TASK_STATUS_WORKING
STATUS_COMPLETED = pb.TASK_STATUS_COMPLETED
STATUS_FAILED    = pb.TASK_STATUS_FAILED
STATUS_CANCELLED = pb.TASK_STATUS_CANCELLED

__all__ = [
    "A2AClient",
    "pb",
    "STATUS_SUBMITTED",
    "STATUS_WORKING",
    "STATUS_COMPLETED",
    "STATUS_FAILED",
    "STATUS_CANCELLED",
    "CapabilityId",
    "CapabilityTag",
    "CoreCapability",
    "CORE_CAPABILITY_PREFIX",
    "capability_id",
    "capability_name",
    "is_core_capability",
    "normalize_capability",
    "normalize_did",
    "short_did",
    "HealthResponse",
    "PeerInfo",
    "PingResponse",
    "TopicMessage",
    "NetworkInfo",
    "NetworkMember",
    "BroadcastMessage",
]

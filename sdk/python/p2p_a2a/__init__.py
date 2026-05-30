from p2p_a2a.client import A2AClient
from p2p_a2a.capability import (
    CapabilityId,
    CapabilityTag,
    CoreCapability,
    CORE_CAPABILITY_PREFIX,
    capability_id,
    capability_name,
    is_core_capability,
    normalize_capability,
)
from p2p_a2a.did import normalize_did, short_did
from p2p_a2a.proto import a2a_pb2 as pb

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
]

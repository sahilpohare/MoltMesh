"""Capability helpers for p2p-a2a."""

from __future__ import annotations

from enum import Enum
from typing import NewType

CapabilityId = NewType("CapabilityId", str)
CapabilityTag = NewType("CapabilityTag", str)

CORE_CAPABILITY_PREFIX = "a2a:v1:cap:"


class CoreCapability(str, Enum):
    TEXT_GENERATION = f"{CORE_CAPABILITY_PREFIX}text-generation"
    CODE_EXECUTION = f"{CORE_CAPABILITY_PREFIX}code-execution"
    WEB_RETRIEVAL = f"{CORE_CAPABILITY_PREFIX}web-retrieval"
    IMAGE_GENERATION = f"{CORE_CAPABILITY_PREFIX}image-generation"
    DATA_ANALYSIS = f"{CORE_CAPABILITY_PREFIX}data-analysis"
    FILE_PROCESSING = f"{CORE_CAPABILITY_PREFIX}file-processing"
    TOOL_USE = f"{CORE_CAPABILITY_PREFIX}tool-use"
    EMBEDDING = f"{CORE_CAPABILITY_PREFIX}embedding"
    SPEECH_TO_TEXT = f"{CORE_CAPABILITY_PREFIX}speech-to-text"
    TEXT_TO_SPEECH = f"{CORE_CAPABILITY_PREFIX}text-to-speech"


def capability_id(name: str, *, version: str = "v1") -> str:
    """
    Build a capability ID. If already namespaced, returns as-is.

    Examples:
      - "text-generation" -> "a2a:v1:cap:text-generation"
      - "acme:cap:legal"  -> "acme:cap:legal"
    """
    if not name:
        return name
    if ":" in name:
        return name
    return f"a2a:{version}:cap:{name}"


def normalize_capability(capability: str, *, version: str = "v1") -> str:
    """Alias for capability_id()."""
    return capability_id(capability, version=version)


def capability_name(capability: str) -> str:
    """Strip the namespace prefix for display."""
    if not capability:
        return capability
    marker = ":cap:"
    if marker not in capability:
        return capability
    return capability.split(marker, 1)[1]


def is_core_capability(capability: str, *, version: str = "v1") -> bool:
    """Return True for core (a2a:*:cap:*) capabilities."""
    if not capability:
        return False
    return capability.startswith(f"a2a:{version}:cap:")

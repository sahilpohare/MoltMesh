"""
moltmesh.ext_stub — Raw gRPC stubs for the Diag and Ext services.

These hand-written stubs mirror the Go gen/a2a/v1/diag.go and
gen/a2a/v1/extensions.go files.  They use generic proto serialisation
(bytes-passthrough) because we don't have protoc-gen-python stubs for
the new services yet.  The client layer deserialises results from the
JSON-compatible dict that grpc returns when longs/enums are strings.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Iterator

import grpc


# ── Simple dataclasses (mirror pb structs) ────────────────────────────────────

@dataclass
class PingResult:
    did: str = ""
    latency_ms: float = 0.0
    reachable: bool = False
    error: str = ""


@dataclass
class PeerInfo:
    peer_id: str = ""
    multiaddrs: list[str] = field(default_factory=list)
    did: str = ""


@dataclass
class HealthInfo:
    version: str = ""
    did: str = ""
    peer_count: int = 0
    uptime_secs: float = 0.0


@dataclass
class TopicMessage:
    topic: str = ""
    payload: bytes = b""
    emitted_at: int = 0


@dataclass
class NetworkInfo:
    id: str = ""
    name: str = ""
    creator_did: str = ""
    created_at: int = 0


@dataclass
class NetworkMember:
    did: str = ""
    joined_at: int = 0


@dataclass
class BroadcastMessage:
    network_id: str = ""
    payload: bytes = b""
    emitted_at: int = 0


# ── JSON-wire stubs ───────────────────────────────────────────────────────────
# grpc-python with proto-loader / raw channels returns dicts for JSON codec.
# We use a thin wrapper that serialises to JSON bytes and back.

class _JsonCodec:
    """Encode/decode using JSON (no protobuf descriptor needed)."""

    @staticmethod
    def encode(msg: dict) -> bytes:
        return json.dumps(msg).encode()

    @staticmethod
    def decode(data: bytes) -> dict:
        return json.loads(data)


def _raw_channel(addr: str) -> grpc.Channel:
    """Open an insecure channel with JSON codec."""
    return grpc.insecure_channel(addr)


class DiagStub:
    """Thin stub for the Diag gRPC service."""

    def __init__(self, channel: grpc.Channel) -> None:
        self.Ping = channel.unary_unary(
            "/a2a.v1.A2ANode/Ping",
            request_serializer=lambda d: json.dumps(d).encode(),
            response_deserializer=lambda b: json.loads(b),
        )
        self.Health = channel.unary_unary(
            "/a2a.v1.A2ANode/Health",
            request_serializer=lambda d: json.dumps(d).encode(),
            response_deserializer=lambda b: json.loads(b),
        )
        self.ListPeers = channel.unary_unary(
            "/a2a.v1.A2ANode/ListPeers",
            request_serializer=lambda d: json.dumps(d).encode(),
            response_deserializer=lambda b: json.loads(b),
        )


class ExtStub:
    """Thin stub for the Ext gRPC service (PubSub, Webhook, Networks)."""

    def __init__(self, channel: grpc.Channel) -> None:
        self.Publish = channel.unary_unary(
            "/a2a.v1.A2ANode/Publish",
            request_serializer=lambda d: json.dumps(d).encode(),
            response_deserializer=lambda b: json.loads(b),
        )
        self.SubscribeTopic = channel.unary_stream(
            "/a2a.v1.A2ANode/SubscribeTopic",
            request_serializer=lambda d: json.dumps(d).encode(),
            response_deserializer=lambda b: json.loads(b),
        )
        self.SetWebhook = channel.unary_unary(
            "/a2a.v1.A2ANode/SetWebhook",
            request_serializer=lambda d: json.dumps(d).encode(),
            response_deserializer=lambda b: json.loads(b),
        )
        self.ClearWebhook = channel.unary_unary(
            "/a2a.v1.A2ANode/ClearWebhook",
            request_serializer=lambda d: json.dumps(d).encode(),
            response_deserializer=lambda b: json.loads(b),
        )
        self.GetWebhook = channel.unary_unary(
            "/a2a.v1.A2ANode/GetWebhook",
            request_serializer=lambda d: json.dumps(d).encode(),
            response_deserializer=lambda b: json.loads(b),
        )
        self.CreateNetwork = channel.unary_unary(
            "/a2a.v1.A2ANode/CreateNetwork",
            request_serializer=lambda d: json.dumps(d).encode(),
            response_deserializer=lambda b: json.loads(b),
        )
        self.JoinNetwork = channel.unary_unary(
            "/a2a.v1.A2ANode/JoinNetwork",
            request_serializer=lambda d: json.dumps(d).encode(),
            response_deserializer=lambda b: json.loads(b),
        )
        self.LeaveNetwork = channel.unary_unary(
            "/a2a.v1.A2ANode/LeaveNetwork",
            request_serializer=lambda d: json.dumps(d).encode(),
            response_deserializer=lambda b: json.loads(b),
        )
        self.ListNetworks = channel.unary_unary(
            "/a2a.v1.A2ANode/ListNetworks",
            request_serializer=lambda d: json.dumps(d).encode(),
            response_deserializer=lambda b: json.loads(b),
        )
        self.NetworkMembers = channel.unary_unary(
            "/a2a.v1.A2ANode/NetworkMembers",
            request_serializer=lambda d: json.dumps(d).encode(),
            response_deserializer=lambda b: json.loads(b),
        )
        self.BroadcastNetwork = channel.unary_unary(
            "/a2a.v1.A2ANode/BroadcastNetwork",
            request_serializer=lambda d: json.dumps(d).encode(),
            response_deserializer=lambda b: json.loads(b),
        )
        self.SubscribeNetwork = channel.unary_stream(
            "/a2a.v1.A2ANode/SubscribeNetwork",
            request_serializer=lambda d: json.dumps(d).encode(),
            response_deserializer=lambda b: json.loads(b),
        )

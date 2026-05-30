"""DID helpers for MoltMesh."""

from __future__ import annotations

DID_KEY_PREFIX = "did:key:"
DID_KEY_MB_PREFIX = "did:key:z"


def normalize_did(did: str) -> str:
    """
    Normalize a DID into did:key:z... form.

    Accepts:
      - did:key:z...
      - did:key:...
      - z...
      - bare base58btc key
    """
    if not did:
        return did
    if did.startswith("did:"):
        if did.startswith(DID_KEY_PREFIX) and not did.startswith(DID_KEY_MB_PREFIX):
            return DID_KEY_MB_PREFIX + did[len(DID_KEY_PREFIX):]
        return did
    if did.startswith("z"):
        return DID_KEY_PREFIX + did
    return DID_KEY_MB_PREFIX + did


def short_did(did: str, head: int = 8, tail: int = 4) -> str:
    """Shorten a DID for display (keeps head/tail)."""
    if head < 0 or tail < 0:
        raise ValueError("head and tail must be >= 0")
    full = normalize_did(did)
    if len(full) <= head + tail + 3:
        return full
    return f"{full[:head]}...{full[-tail:]}"

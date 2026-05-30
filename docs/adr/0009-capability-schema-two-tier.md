# ADR-0009: Two-Tier Capability Schema

**Status**: Accepted
**Date**: 2026-05-30

## Context

Agents need to advertise what they can do, and other agents need to find them by capability. Two extremes debated:

- **Free-form tags**: easy to publish, impossible to search reliably (DHT is exact-key, not semantic)
- **Rigid ontology**: requires governance body, freezes ecosystem evolution

## Decision

**Two-tier capability schema:**

### Tier 1 — Core Ontology (DHT-indexed)
A small, versioned set of well-known capability namespaces. DHT key = `sha256("a2a:v1:cap:{name}")`.

Initial core capabilities:
```
a2a:v1:cap:text-generation
a2a:v1:cap:code-execution
a2a:v1:cap:web-retrieval
a2a:v1:cap:image-generation
a2a:v1:cap:data-analysis
a2a:v1:cap:file-processing
a2a:v1:cap:tool-use
a2a:v1:cap:embedding
a2a:v1:cap:speech-to-text
a2a:v1:cap:text-to-speech
```

Agents advertising a core capability are indexed in the DHT under that key.

### Tier 2 — Namespaced Extensions (Agent Card only, not DHT-indexed)
Agents can declare additional capabilities in their own namespace:
```
acme:cap:financial-analysis-v2
anthropic:model:claude-sonnet-4-6
myorg:cap:tax-law-summarization
```

These appear in the Agent Card only. Discovery happens via two-phase lookup:
1. DHT search by Tier 1 key → list of candidate agent DIDs
2. Fetch Agent Cards for candidates → filter by Tier 2 tags

## Capability Record in Agent Card

```json
{
  "skills": [
    {
      "id": "a2a:v1:cap:text-generation",
      "name": "Text Generation",
      "description": "...",
      "inputSchema": { ... },
      "outputSchema": { ... },
      "tags": ["anthropic:model:claude-sonnet-4-6", "acme:cap:legal-text"]
    }
  ]
}
```

## Rationale

- DHT exact-key matching requires deterministic keys. Free-form tags produce fragmented namespaces within months.
- A small core ontology (10-15 capabilities) covers 80% of use cases. Community standards emerge bottom-up for the rest (like npm namespaces, Docker Hub).
- Two-phase discovery (coarse DHT → fine Agent Card) is the correct architecture — don't pack all selection criteria into DHT keys.
- Versioned core (`a2a:v1:cap:*`) allows future evolution without breaking existing registrations.

## Consequences

- Agents must classify themselves under at least one Tier 1 capability to be discoverable via DHT search.
- Core ontology changes require a version bump (`a2a:v2:cap:*`) — old agents remain discoverable under v1 keys.
- Tier 2 tags are unverified — any agent can claim any namespace. Trust/attestation for Tier 2 is a v2 concern.

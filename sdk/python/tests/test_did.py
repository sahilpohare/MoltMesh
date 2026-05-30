"""Tests for moltmesh.did helpers."""

import pytest

from moltmesh.did import normalize_did, short_did


class TestNormalizeDid:
    def test_empty(self):
        assert normalize_did("") == ""

    def test_already_normalized(self):
        did = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
        assert normalize_did(did) == did

    def test_missing_multibase_z(self):
        did = "did:key:6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
        assert normalize_did(did) == "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"

    def test_bare_z_prefixed(self):
        key = "z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
        assert normalize_did(key) == "did:key:" + key

    def test_bare_base58(self):
        key = "6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
        assert normalize_did(key) == "did:key:z" + key

    def test_non_key_did_passthrough(self):
        did = "did:web:example.com"
        assert normalize_did(did) == did


class TestShortDid:
    def test_short_did(self):
        did = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
        result = short_did(did)
        assert result.startswith("did:key:")
        assert "..." in result
        assert len(result) < len(did)

    def test_short_did_already_short(self):
        did = "did:key:z6Mk"
        result = short_did(did, head=20, tail=10)
        assert result == did  # too short to truncate

    def test_custom_head_tail(self):
        did = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
        result = short_did(did, head=12, tail=6)
        assert result[:12] == did[:12]
        assert result[-6:] == did[-6:]
        assert "..." in result

    def test_negative_head_raises(self):
        with pytest.raises(ValueError):
            short_did("did:key:z123", head=-1)

    def test_negative_tail_raises(self):
        with pytest.raises(ValueError):
            short_did("did:key:z123", tail=-1)

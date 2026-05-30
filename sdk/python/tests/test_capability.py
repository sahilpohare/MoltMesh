"""Tests for moltmesh.capability helpers."""

from moltmesh.capability import (
    capability_id,
    capability_name,
    normalize_capability,
    is_core_capability,
    CoreCapability,
    CORE_CAPABILITY_PREFIX,
)


class TestCapabilityId:
    def test_bare_name(self):
        assert capability_id("text-generation") == "a2a:v1:cap:text-generation"

    def test_already_namespaced(self):
        assert capability_id("acme:cap:legal") == "acme:cap:legal"

    def test_empty(self):
        assert capability_id("") == ""

    def test_custom_version(self):
        assert capability_id("foo", version="v2") == "a2a:v2:cap:foo"


class TestCapabilityName:
    def test_strip_prefix(self):
        assert capability_name("a2a:v1:cap:text-generation") == "text-generation"

    def test_custom_namespace(self):
        assert capability_name("acme:cap:legal") == "legal"

    def test_no_cap_marker(self):
        assert capability_name("plain") == "plain"

    def test_empty(self):
        assert capability_name("") == ""


class TestIsCoreCapability:
    def test_core(self):
        assert is_core_capability("a2a:v1:cap:text-generation") is True

    def test_non_core(self):
        assert is_core_capability("acme:v1:cap:custom") is False

    def test_empty(self):
        assert is_core_capability("") is False

    def test_wrong_version(self):
        assert is_core_capability("a2a:v2:cap:text-generation", version="v1") is False
        assert is_core_capability("a2a:v2:cap:text-generation", version="v2") is True


class TestNormalizeCapability:
    def test_alias(self):
        assert normalize_capability("embedding") == capability_id("embedding")


class TestCoreCapabilityEnum:
    def test_values(self):
        assert CoreCapability.TEXT_GENERATION == "a2a:v1:cap:text-generation"
        assert CoreCapability.EMBEDDING == "a2a:v1:cap:embedding"

    def test_prefix(self):
        for cap in CoreCapability:
            assert cap.value.startswith(CORE_CAPABILITY_PREFIX)

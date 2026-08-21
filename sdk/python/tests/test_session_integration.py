from moltmesh import A2AClient, AgentIdentity
from moltmesh.proto import a2a_pb2 as pb


def test_sdk_identity_authenticates_to_daemon_and_is_scoped(client, tmp_path):
    identity = AgentIdentity.load_or_create(tmp_path / "sdk-agent")
    with A2AClient(client._addr, identity=identity) as scoped:
        assert scoped.agent_did == identity.did
        assert scoped.node_peer_id
        got = scoped.stub.GetAgentIdentity(pb.Empty())
        assert got.did == identity.did
        assert got.signing_public_key == identity.signing_public_key
        assert got.encryption_public_key == identity.encryption_public_key

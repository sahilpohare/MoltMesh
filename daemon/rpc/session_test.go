package rpc_test

import (
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	"github.com/sahilpohare/p2p-a2a/daemon/session"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

func TestAgentSessionChallengeIsBoundSingleUseAndAuthenticated(t *testing.T) {
	env := newTestEnv(t)
	agent, created, challenge, signature := createSession(t, env)
	if created.Token == "" || created.AgentDid != agent.Did {
		t.Fatalf("unexpected session: %#v", created)
	}

	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+created.Token)
	got, err := env.client.GetAgentIdentity(ctx, &pb.Empty{})
	if err != nil || got.Did != agent.Did {
		t.Fatalf("GetAgentIdentity = %#v, %v", got, err)
	}
	if _, err := env.client.CompleteAgentSession(context.Background(), &pb.CompleteAgentSessionRequest{ChallengeId: challenge.ChallengeId, Signature: signature}); err == nil {
		t.Fatal("replayed session challenge was accepted")
	}
	if _, err := env.client.GetAgentIdentity(context.Background(), &pb.Empty{}); err == nil {
		t.Fatal("unscoped GetAgentIdentity was accepted")
	}
}

func TestSessionAgentCannotReadAnotherAgentsTask(t *testing.T) {
	env := newTestEnv(t)
	_, first, _, _ := createSession(t, env)
	_, second, _, _ := createSession(t, env)
	firstCtx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+first.Token)
	secondCtx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+second.Token)
	task, err := env.client.CreateTask(firstCtx, &pb.CreateTaskRequest{ToDid: "did:key:zRemote", Task: &pb.TaskRequest{Skill: "calculator"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.client.GetTask(firstCtx, &pb.TaskID{Id: task.Id}); err != nil {
		t.Fatalf("owner could not read task: %v", err)
	}
	if _, err := env.client.GetTask(secondCtx, &pb.TaskID{Id: task.Id}); err == nil {
		t.Fatal("unrelated agent could read another agent's task")
	}
}

func TestLegacyRequestIsRejectedWhenTwoSDKAgentsAreConnected(t *testing.T) {
	env := newTestEnv(t)
	_, _, _, _ = createSession(t, env)
	_, _, _, _ = createSession(t, env)
	if _, err := env.client.CreateTask(context.Background(), &pb.CreateTaskRequest{ToDid: "did:key:zRemote", Task: &pb.TaskRequest{Skill: "calculator"}}); err == nil {
		t.Fatal("expected ambiguous unscoped request to be rejected")
	}
}

func createSession(t *testing.T, env *testEnv) (*pb.AgentIdentity, *pb.AgentSession, *pb.AgentChallenge, []byte) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	x25519, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	agent := &pb.AgentIdentity{Did: identity.DIDFromPubBytes(pub), SigningPublicKey: pub, EncryptionPublicKey: x25519.PublicKey().Bytes()}
	challenge, err := env.client.BeginAgentSession(context.Background(), &pb.BeginAgentSessionRequest{Identity: agent})
	if err != nil {
		t.Fatalf("BeginAgentSession: %v", err)
	}
	signature := ed25519.Sign(priv, session.SigningPayload(env.id.DID, challenge.ChallengeId, challenge.Nonce, time.UnixMilli(challenge.ExpiresAtUnixMs), agent.Did))
	created, err := env.client.CompleteAgentSession(context.Background(), &pb.CompleteAgentSessionRequest{ChallengeId: challenge.ChallengeId, Signature: signature})
	if err != nil {
		t.Fatalf("CompleteAgentSession: %v", err)
	}
	return agent, created, challenge, signature
}

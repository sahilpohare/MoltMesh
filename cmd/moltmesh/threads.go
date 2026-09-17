package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"google.golang.org/protobuf/proto"
)

func cmdCreateThread(args []string) error {
	fs := flag.NewFlagSet("create-thread", flag.ExitOnError)
	dataDir, grpcAddr := clientFlags(fs)
	replicas := fs.String("replicas", "", "Comma-separated replica DIDs")
	f := fs.Int("f", 0, "Max byzantine faults to tolerate")
	epochMs := fs.Int64("epoch-ms", 0, "Timeout propose in ms")
	withRecovery := fs.Bool("with-recovery", false, "Create and return a portable recovery capability")
	backend := fs.String("backend", "", "Consensus backend: raft (default) or tendermint")
	fs.Parse(args)

	var replicaDIDs []string
	if *replicas != "" {
		for _, r := range strings.Split(*replicas, ",") {
			r = strings.TrimSpace(r)
			if r != "" {
				replicaDIDs = append(replicaDIDs, r)
			}
		}
	}

	client, conn, err := connect(*dataDir, *grpcAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	req := &pb.CreateThreadRequest{
		ReplicaDids: replicaDIDs,
		F:           int32(*f),
		EpochMs:     *epochMs,
	}
	if *backend != "" {
		req.Metadata = map[string]string{"backend": *backend}
	}
	if *withRecovery {
		response, err := client.CreateThreadWithRecovery(context.Background(), req)
		if err != nil {
			return err
		}
		data, _ := json.MarshalIndent(response, "", "  ")
		fmt.Println(string(data))
		return nil
	}
	thread, err := client.CreateThread(context.Background(), req)
	if err != nil {
		return err
	}
	data, _ := json.MarshalIndent(thread, "", "  ")
	fmt.Println(string(data))
	return nil
}

func cmdGetThread(args []string) error {
	fs := flag.NewFlagSet("get-thread", flag.ExitOnError)
	dataDir, grpcAddr := clientFlags(fs)
	id := fs.String("id", "", "Thread ID (required)")
	fs.Parse(args)

	if *id == "" {
		return fmt.Errorf("--id is required")
	}

	client, conn, err := connect(*dataDir, *grpcAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	thread, err := client.GetThread(context.Background(), &pb.ThreadID{Id: *id})
	if err != nil {
		return err
	}

	data, _ := json.MarshalIndent(thread, "", "  ")
	fmt.Println(string(data))
	return nil
}

func cmdAppendEntry(args []string) error {
	fs := flag.NewFlagSet("append-entry", flag.ExitOnError)
	dataDir, grpcAddr := clientFlags(fs)
	threadID := fs.String("thread-id", "", "Thread ID (required)")
	payload := fs.String("payload", "", "Entry payload (string)")
	kind := fs.String("kind", "custom", "Entry kind")
	fs.Parse(args)

	if *threadID == "" {
		return fmt.Errorf("--thread-id is required")
	}

	client, conn, err := connect(*dataDir, *grpcAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	result, err := client.AppendEntry(context.Background(), &pb.AppendEntryRequest{
		ThreadId: *threadID,
		Payload:  []byte(*payload),
		Kind:     *kind,
	})
	if err != nil {
		return err
	}

	data, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(data))
	return nil
}

func cmdGetThreadEntries(args []string) error {
	fs := flag.NewFlagSet("get-thread-entries", flag.ExitOnError)
	dataDir, grpcAddr := clientFlags(fs)
	id := fs.String("id", "", "Thread ID (required)")
	since := fs.Int64("since", 0, "Since height")
	limit := fs.Int("limit", 50, "Max entries")
	fs.Parse(args)

	if *id == "" {
		return fmt.Errorf("--id is required")
	}

	client, conn, err := connect(*dataDir, *grpcAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	stream, err := client.GetThreadEntries(context.Background(), &pb.GetThreadEntriesRequest{
		ThreadId:    *id,
		SinceHeight: *since,
		Limit:       int32(*limit),
	})
	if err != nil {
		return err
	}

	count := 0
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		data, _ := json.MarshalIndent(entry, "", "  ")
		fmt.Println(string(data))
		count++
	}
	if count == 0 {
		fmt.Println("No entries.")
	}
	return nil
}

func cmdSubscribeThread(args []string) error {
	fs := flag.NewFlagSet("subscribe-thread", flag.ExitOnError)
	dataDir, grpcAddr := clientFlags(fs)
	id := fs.String("id", "", "Thread ID (required)")
	since := fs.Int64("since", 0, "Since height")
	decode := fs.Bool("decode", false, "Emit one JSON object per entry, payload decoded as text")
	fs.Parse(args)

	if *id == "" {
		return fmt.Errorf("--id is required")
	}

	client, conn, err := connect(*dataDir, *grpcAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	fmt.Fprintln(os.Stderr, "Subscribing to thread (Ctrl+C to stop)...")
	stream, err := client.SubscribeThread(context.Background(), &pb.SubscribeThreadRequest{
		ThreadId:    *id,
		SinceHeight: *since,
	})
	if err != nil {
		return err
	}

	// --decode emits newline-delimited JSON with the payload as text, so a
	// watcher can read each committed entry as one line.
	enc := json.NewEncoder(os.Stdout)
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if *decode {
			out := map[string]interface{}{"height": entry.Height, "entry": entry.Entry}
			if entry.Entry != nil {
				out["text"] = string(entry.Entry.Payload)
			}
			if err := enc.Encode(out); err != nil {
				return err
			}
			continue
		}
		data, _ := json.MarshalIndent(entry, "", "  ")
		fmt.Println(string(data))
	}
	return nil
}

// cmdAddThreadReplica adds an observer DID as a replica of a thread. Ported
// from cmd/daemon's add-thread-replica command (retired) so cmd/moltmesh
// remains a strict superset of it.
// cmdPromoteThreadMember promotes an existing observer to a voting member so
// it can write to the thread. Promotion needs a catchup proof signed by the
// observer itself, so this runs in two steps against two daemons: the
// observer's daemon reports and signs its committed head, then the creator's
// daemon verifies that signature against its own head and commits the voter
// change through Raft.
func cmdPromoteThreadMember(args []string) error {
	fs := flag.NewFlagSet("promote-thread-member", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "Creator data directory")
	grpcAddr := fs.String("grpc-addr", "", "gRPC server address")
	threadID := fs.String("thread-id", "", "Thread ID (required)")
	memberDataDir := fs.String("member-data-dir", "", "Observer's data directory (required; used to sign its catchup proof)")
	fs.Parse(args)

	if *threadID == "" {
		return fmt.Errorf("--thread-id is required")
	}
	if *memberDataDir == "" {
		return fmt.Errorf("--member-data-dir is required")
	}

	memberDir, err := filepath.Abs(*memberDataDir)
	if err != nil {
		return err
	}
	memberID, err := identity.Load(filepath.Join(memberDir, "identity.json"))
	if err != nil {
		return fmt.Errorf("load observer identity: %w", err)
	}

	// Step 1: ask the observer's own daemon where it has committed to.
	memberConn, err := dialGRPC(resolveGRPCAddr("", memberDir))
	if err != nil {
		return fmt.Errorf("dial observer daemon: %w", err)
	}
	defer memberConn.Close()
	state, err := pb.NewA2ANodeClient(memberConn).GetThreadCatchupState(
		context.Background(), &pb.ThreadID{Id: *threadID})
	if err != nil {
		return fmt.Errorf("get observer catchup state: %w", err)
	}

	// Step 3 verifies this proof against the creator's committed head, so it is
	// invalid until the observer has caught up. Wait for that here rather than
	// making the caller retry a race.
	dir, err := resolveDataDir(*dataDir)
	if err != nil {
		return err
	}
	conn, err := dialGRPC(resolveGRPCAddr(*grpcAddr, dir))
	if err != nil {
		return err
	}
	defer conn.Close()
	creator := pb.NewA2ANodeClient(conn)

	deadline := time.Now().Add(60 * time.Second)
	for {
		head, hErr := creator.GetThreadCatchupState(context.Background(), &pb.ThreadID{Id: *threadID})
		if hErr != nil {
			break // let the promotion call surface the real error
		}
		if state.CommittedHeight >= head.CommittedHeight && state.HeadBlockHash == head.HeadBlockHash {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("observer did not catch up to committed height %d within 60s (at %d)",
				head.CommittedHeight, state.CommittedHeight)
		}
		time.Sleep(500 * time.Millisecond)
		state, err = pb.NewA2ANodeClient(memberConn).GetThreadCatchupState(
			context.Background(), &pb.ThreadID{Id: *threadID})
		if err != nil {
			return fmt.Errorf("get observer catchup state: %w", err)
		}
	}

	// Step 2: the observer signs that state. The server re-marshals with the
	// signature cleared, so sign exactly the same deterministic bytes.
	proof := &pb.ThreadCatchupProof{
		ThreadId:        state.ThreadId,
		ObserverDid:     state.ObserverDid,
		CommittedHeight: state.CommittedHeight,
		HeadBlockHash:   state.HeadBlockHash,
		IssuedAtUnixMs:  time.Now().UnixMilli(),
	}
	signable, err := proto.MarshalOptions{Deterministic: true}.Marshal(proof)
	if err != nil {
		return err
	}
	proof.Signature = memberID.Sign(signable)
	raw, err := proto.Marshal(proof)
	if err != nil {
		return err
	}

	// Step 3: the creator verifies and commits the voter change.
	change, err := creator.PromoteThreadMember(context.Background(),
		&pb.PromoteThreadMemberRequest{
			ThreadId:     *threadID,
			MemberDid:    state.ObserverDid,
			CatchupProof: raw,
		})
	if err != nil {
		return err
	}
	jsonOut(change)
	return nil
}

func cmdAddThreadReplica(args []string) error {
	fs := flag.NewFlagSet("add-thread-replica", flag.ExitOnError)
	dataDir, grpcAddr := clientFlags(fs)
	threadID := fs.String("thread-id", "", "Thread ID")
	did := fs.String("did", "", "Observer DID")
	fs.Parse(args)
	if *threadID == "" || *did == "" {
		return fmt.Errorf("--thread-id and --did are required")
	}
	dir, err := resolveDataDir(*dataDir)
	if err != nil {
		return err
	}
	conn, err := dialGRPC(resolveGRPCAddr(*grpcAddr, dir))
	if err != nil {
		return err
	}
	defer conn.Close()
	th, err := pb.NewA2ANodeClient(conn).AddThreadReplica(context.Background(), &pb.ThreadReplicaRequest{ThreadId: *threadID, ReplicaDid: *did})
	if err != nil {
		return err
	}
	jsonOut(th)
	return nil
}

// cmdRecoverThread imports verified read-only history using the complete
// bearer capability. A public thread ID alone is deliberately insufficient.
func cmdRecoverThread(args []string) error {
	fs := flag.NewFlagSet("recover-thread", flag.ExitOnError)
	dataDir, grpcAddr := clientFlags(fs)
	id := fs.String("id", "", "Thread ID")
	secretB64 := fs.String("secret-base64", "", "Base64 recovery secret (required)")
	fs.Parse(args)
	if *id == "" || *secretB64 == "" {
		return fmt.Errorf("--id and --secret-base64 are required")
	}
	secret, err := base64.StdEncoding.DecodeString(*secretB64)
	if err != nil || len(secret) != 32 {
		return fmt.Errorf("--secret-base64 must decode to a 32-byte recovery secret")
	}
	dir, err := resolveDataDir(*dataDir)
	if err != nil {
		return err
	}
	conn, err := dialGRPC(resolveGRPCAddr(*grpcAddr, dir))
	if err != nil {
		return err
	}
	defer conn.Close()
	response, err := pb.NewA2ANodeClient(conn).RecoverThreadWithHandle(context.Background(), &pb.RecoverThreadRequest{
		Handle: &pb.ThreadRecoveryHandle{ThreadId: *id, RecoverySecret: secret, Version: 1},
	})
	if err != nil {
		return err
	}
	jsonOut(response)
	return nil
}

// ── Diagnostics ───────────────────────────────────────────────────────────────

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

func cmdSendFile(args []string) error {
	fs := flag.NewFlagSet("send-file", flag.ExitOnError)
	dataDir, grpcAddr := clientFlags(fs)
	filePath := fs.String("file", "", "File to upload (required)")
	mimeType := fs.String("mime-type", "application/octet-stream", "MIME type")
	fs.Parse(args)

	if *filePath == "" {
		return fmt.Errorf("--file is required")
	}

	fileData, err := os.ReadFile(*filePath)
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}

	client, conn, err := connect(*dataDir, *grpcAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	artifact, err := client.SendFile(context.Background(), &pb.SendFileRequest{
		Data:     fileData,
		Name:     *filePath,
		MimeType: *mimeType,
	})
	if err != nil {
		return err
	}

	data, _ := json.MarshalIndent(artifact, "", "  ")
	fmt.Println(string(data))
	return nil
}

func cmdFetchFile(args []string) error {
	fs := flag.NewFlagSet("fetch-file", flag.ExitOnError)
	dataDir, grpcAddr := clientFlags(fs)
	cid := fs.String("cid", "", "Content ID to fetch (required)")
	from := fs.String("from", "", "Source DID (required)")
	out := fs.String("out", "", "Output file path (default: stdout)")
	fs.Parse(args)

	if *cid == "" {
		return fmt.Errorf("--cid is required")
	}
	if *from == "" {
		return fmt.Errorf("--from is required")
	}

	client, conn, err := connect(*dataDir, *grpcAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	stream, err := client.FetchFile(context.Background(), &pb.FetchFileRequest{
		Cid:     *cid,
		FromDid: *from,
	})
	if err != nil {
		return err
	}

	var w io.Writer = os.Stdout
	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			return fmt.Errorf("create output file: %w", err)
		}
		defer f.Close()
		w = f
	}

	var total int64
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if _, err := w.Write(chunk.Data); err != nil {
			return err
		}
		total = chunk.Total
	}

	if *out != "" {
		fmt.Fprintf(os.Stderr, "Saved %d bytes to %s\n", total, *out)
	}
	return nil
}

// ── Threads ───────────────────────────────────────────────────────────────────

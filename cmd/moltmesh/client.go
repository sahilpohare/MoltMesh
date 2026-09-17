package main

import (
	"flag"

	"google.golang.org/grpc"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

// clientFlags registers the two flags every daemon-talking command shares.
func clientFlags(fs *flag.FlagSet) (dataDir, grpcAddr *string) {
	return fs.String("data-dir", "", "Data directory"),
		fs.String("grpc-addr", "", "gRPC server address")
}

// connect resolves the daemon from the shared flags and dials it. The caller
// closes the returned connection.
func connect(dataDir, grpcAddr string) (pb.A2ANodeClient, *grpc.ClientConn, error) {
	dir, err := resolveDataDir(dataDir)
	if err != nil {
		return nil, nil, err
	}
	conn, err := dialGRPC(resolveGRPCAddr(grpcAddr, dir))
	if err != nil {
		return nil, nil, err
	}
	return pb.NewA2ANodeClient(conn), conn, nil
}

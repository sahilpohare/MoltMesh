package rpc

// version is set at build time or defaulted here.
// It is injected by cmd/moltmesh/daemon.go via the rpc.SetVersion call.
var version = "0.1.0"

// SetVersion allows the main package to inject the build version.
func SetVersion(v string) {
	version = v
}

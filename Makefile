BINARY     := p2p-a2a
PROTO_DIR  := proto
GEN_DIR    := gen/a2a/v1
GOPATH_BIN := $(shell go env GOPATH)/bin

.PHONY: all build proto clean run test

all: proto build

STRAY_GEN  := github.com/sahilpohare/p2p-a2a/gen/a2a/v1

proto:
	PATH="$$PATH:$(GOPATH_BIN)" protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=. \
		--go_opt=paths=import \
		--go-grpc_out=. \
		--go-grpc_opt=paths=import \
		$(PROTO_DIR)/a2a.proto
	cp $(STRAY_GEN)/a2a.pb.go      $(GEN_DIR)/a2a.pb.go
	cp $(STRAY_GEN)/a2a_grpc.pb.go $(GEN_DIR)/a2a_grpc.pb.go

build:
	go build -o $(BINARY) ./cmd/daemon

run: build
	A2A_DATA_DIR=./.data ./$(BINARY)

test:
	go test ./...

clean:
	rm -f $(BINARY)
	rm -rf .data

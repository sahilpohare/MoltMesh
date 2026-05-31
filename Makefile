BINARY     := moltmesh
PROTO_DIR  := proto
GEN_DIR    := gen/a2a/v1
GOPATH_BIN := $(shell go env GOPATH)/bin
VERSION    ?= dev
LDFLAGS    := -s -w -X main.version=$(VERSION)

.PHONY: all build build-linux build-darwin proto clean run test install

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
	go build -ldflags "$(LDFLAGS)" -o $(BINARY) ./cmd/moltmesh

build-linux:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(BINARY)-linux-amd64 ./cmd/moltmesh

build-darwin:
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(BINARY)-darwin-arm64 ./cmd/moltmesh

install:
	go install -ldflags "$(LDFLAGS)" ./cmd/moltmesh

run: build
	./$(BINARY) start

test:
	go test ./...

clean:
	rm -f $(BINARY) $(BINARY)-linux-amd64 $(BINARY)-darwin-arm64
	rm -rf .data

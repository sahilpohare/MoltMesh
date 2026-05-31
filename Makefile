PROTO_DIR  := proto
GEN_DIR    := gen/a2a/v1
GOPATH_BIN := $(shell go env GOPATH)/bin
VERSION    ?= dev
LDFLAGS    := -s -w -X main.version=$(VERSION)

BINARIES   := moltmesh daemon tui

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
	$(foreach bin,$(BINARIES),go build -ldflags "$(LDFLAGS)" -o $(bin) ./cmd/$(bin);)

build-linux:
	$(foreach bin,$(BINARIES),GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(bin)-linux-amd64 ./cmd/$(bin);)

build-darwin:
	$(foreach bin,$(BINARIES),GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(bin)-darwin-arm64 ./cmd/$(bin);)

install:
	$(foreach bin,$(BINARIES),go install -ldflags "$(LDFLAGS)" ./cmd/$(bin);)

run: build
	./moltmesh start

test:
	go test ./...

clean:
	rm -f $(foreach bin,$(BINARIES),$(bin) $(bin)-linux-amd64 $(bin)-darwin-arm64)
	rm -rf .data

PROTO_DIR  := proto
GEN_DIR    := gen/a2a/v1
GOPATH_BIN := $(shell go env GOPATH)/bin
VERSION    ?= dev
LDFLAGS    := -s -w -X main.version=$(VERSION)
BIN_DIR    := bin

BINARIES   := moltmesh

.PHONY: all build build-all build-linux build-darwin proto clean run test install

all: proto build

proto:
	cd $(PROTO_DIR) && PATH="$$PATH:$(GOPATH_BIN)" protoc \
		--proto_path=. \
		--go_out=../$(GEN_DIR) \
		--go_opt=paths=source_relative \
		--go-grpc_out=../$(GEN_DIR) \
		--go-grpc_opt=paths=source_relative \
		a2a.proto

build:
	@mkdir -p $(BIN_DIR)
	$(foreach bin,$(BINARIES),go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$(bin) ./cmd/$(bin);)

build-all: build-linux build-darwin

build-linux:
	@mkdir -p $(BIN_DIR)
	$(foreach bin,$(BINARIES),GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$(bin)-linux-amd64 ./cmd/$(bin);)

build-darwin:
	@mkdir -p $(BIN_DIR)
	$(foreach bin,$(BINARIES),GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$(bin)-darwin-arm64 ./cmd/$(bin);)

install:
	$(foreach bin,$(BINARIES),go install -ldflags "$(LDFLAGS)" ./cmd/$(bin);)

run: build
	$(BIN_DIR)/moltmesh start

test:
	go test ./...

clean:
	rm -rf $(BIN_DIR)
	rm -rf .data

.DEFAULT_GOAL := help

BINARY  := cachekv
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: help build run test cover lint fmt vet fuzz bench docker clean check

help: ## Show this help
	grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-8s %s\n", $$1, $$2}'

build: ## Build a static binary into ./bin
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/$(BINARY) ./cmd/cachekv

run: ## Run the server on :6379 with debug logs
	go run ./cmd/cachekv -log-level debug

test: ## Run all tests with the race detector
	go test -race -count=1 ./...

cover: ## Run tests and print per-function coverage
	go test -race -count=1 -covermode=atomic -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -n 1

vet: ## Run go vet
	go vet ./...

fmt: ## Format the code
	gofmt -w .

lint: ## Run golangci-lint (must be installed)
	golangci-lint run ./...

fuzz: ## Fuzz the protocol parser for 30 seconds
	go test -run '^$$' -fuzz FuzzReadCommand -fuzztime 30s ./internal/protocol

bench: ## Run the store micro-benchmarks
	go test -run '^$$' -bench . -benchmem ./internal/store

docker: ## Build the container image
	docker build --build-arg VERSION=$(VERSION) -t cachekv:$(VERSION) .

check: vet lint test ## Everything CI runs, locally

clean: ## Remove build artifacts
	rm -rf bin coverage.out

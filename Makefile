VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X github.com/paddo-tech/ushr/internal/version.Version=$(VERSION)

.PHONY: build test lint tidy clean

build:
	@mkdir -p bin
	go build -ldflags "$(LDFLAGS)" -o bin/ushr            ./cmd/ushr
	go build -ldflags "$(LDFLAGS)" -o bin/ushr-controller ./cmd/controller
	go build -ldflags "$(LDFLAGS)" -o bin/ushr-agent      ./cmd/agent

test:
	go test ./...

lint:
	golangci-lint run

tidy:
	go mod tidy

clean:
	rm -rf bin/

.DEFAULT_GOAL := build

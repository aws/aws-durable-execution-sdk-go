.PHONY: build vet lint test check fmt

build:
	go build ./...

vet:
	go vet ./...

lint:
	golangci-lint run ./...

test:
	go test -race ./...

fmt:
	golangci-lint fmt ./...

check: build vet lint test

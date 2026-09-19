.PHONY: build vet lint test check check-all fmt

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

# Run the full CI battery across all modules (durable, insight,
# conformance, examples, analysis) — same checks as .github/workflows/ci.yml.
check-all:
	sh scripts/ci-local.sh

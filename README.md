# AWS Durable Execution SDK for Go

Go SDK for AWS Lambda Durable Functions. Write long-running, fault-tolerant
Lambda functions using checkpointed durable operations.

> **Status: pre-release development.** APIs are not stable. Do not depend on
> this module yet.

## Usage

```go
package main

import (
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

func handler(ctx durable.Context, order Order) (string, error) {
	data, err := durable.Step(ctx, "fetch", fetchData)
	if err != nil {
		return "", err
	}
	if err := durable.Wait(ctx, "cool-down", 30*time.Second); err != nil {
		return "", err
	}
	return process(data), nil
}

func main() {
	durable.Start(handler)
}
```

## Development

Requires Go 1.25 (pinned via `.mise.toml`).

```sh
make build   # compile all packages
make vet     # go vet
make lint    # golangci-lint
make test    # unit tests
make check   # all of the above
```

## License

Apache-2.0. See [LICENSE](LICENSE).

# Contributing to the AWS Durable Execution SDK for Go

Thank you for your interest in contributing! This document explains how to
get started, how the project is organized, and what we expect from
contributions.

## Reporting Bugs

Open a [GitHub issue](../../issues/new) with:

- A clear title describing the problem
- The Go version and SDK version you are using
- Steps to reproduce
- Expected vs. actual behavior
- Relevant log output or stack traces

## Suggesting Features

Open a [GitHub issue](../../issues/new) describing:

- The use case or problem you are solving
- Your proposed solution (API sketch, behavior change, etc.)
- Alternatives you considered

## Development Setup

### Prerequisites

- Go 1.24 or later
- [golangci-lint](https://golangci-lint.run/welcome/install/) (for linting and formatting)

### Clone and Build

```bash
git clone https://github.com/aws/aws-durable-execution-sdk-go.git
cd aws-durable-execution-sdk-go
make check
```

`make check` runs build, vet, lint, and test in sequence. All four must pass
before submitting a pull request.

## Module Structure

The repository is a multi-module Go workspace:

| Path | Description |
|------|-------------|
| `durable/` | Core SDK: handler, context, checkpoint, futures, operations |
| `insight/` | Observability plugin (separate `go.mod`) |
| `compliance/` | Conformance test handlers for cross-SDK validation |
| `examples/` | Self-contained example Lambda functions |

Each module with its own `go.mod` is built and tested independently.

## Running Tests

Run tests with the race detector in each module:

```bash
go test -race ./...
```

Or use the Makefile target from the repository root (covers the root module):

```bash
make test
```

For the `insight/` module:

```bash
cd insight && go test -race ./...
```

## Running CI Checks Locally

The full CI check suite runs locally with a single command:

```bash
sh scripts/ci-local.sh
```

Or through the Makefile wrapper:

```bash
make check-all
```

Both run the following steps in each of the four modules (root/durable,
insight, compliance, examples):

1. `go build ./...`
2. `go vet ./...`
3. `golangci-lint run ./...`
4. `golangci-lint fmt --diff ./...` (fails if any file is unformatted)
5. `go test -race ./...`

The examples module also runs `go vet -tags cloud ./cloud` to check the
cloud test harness compiles.

You can check a single module by passing its path:

```bash
sh scripts/ci-local.sh examples
```

### Tool Versions

Tool versions are pinned in `.mise.toml` at the repository root. Run
`mise install` to install the correct versions:

- Go 1.25 (resolves to the latest patch)
- golangci-lint 2.12.2

### Checks That Cannot Run Locally

The cloud-tests and conformance-tests workflows need AWS credentials and a
deployed CloudFormation stack. They run only in CI with the following
repository secrets:

- `TEST_ROLE_ARN` — IAM role for deploying and invoking test stacks
- `TEST_ACCOUNT_ID` — AWS account hosting the test infrastructure
- `SLACK_WEBHOOK_URL_ISSUE` — Slack notification for opened issues
- `SLACK_WEBHOOK_URL_PR` — Slack notification for opened pull requests

## Code Style

- Format with `gofmt` (enforced by golangci-lint)
- Lint with `golangci-lint run ./...`
- Style precedence: [Effective Go](https://go.dev/doc/effective_go) >
  [Google Go Style Guide](https://google.github.io/styleguide/go/) >
  [Uber Go Style Guide](https://github.com/uber-go/guide/blob/master/style.md)
- Exported symbols require godoc comments
- Unexported helpers should have brief comments explaining non-obvious logic

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <subject>

<body>
```

Types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `perf`, `ci`

Rules:
- Subject line: imperative mood, lowercase, no period, max 50 characters
- Body: wrap at 72 characters, explain what and why

Example:

```
feat(step): Add configurable retry backoff

Allow callers to specify a custom backoff function via WithBackoff.
This enables use cases where the default exponential backoff is too
aggressive for rate-limited downstream services.
```

## Pull Requests

1. Fork the repository
2. Create a feature branch from `main` (`feat/description` or `fix/description`)
3. Make your changes
4. Run `make check` and confirm it passes
5. Commit with a conventional commit message
6. Push to your fork and open a pull request against `main`

Keep pull requests focused on a single concern. If you find unrelated
issues while working, open a separate PR for those.

## License

This project is licensed under the Apache License 2.0. By contributing, you
agree that your contributions will be licensed under the same terms. See the
[LICENSE](LICENSE) file for details.

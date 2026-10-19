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

- Go 1.25 or later. The SDK modules declare `go 1.24` and build with Go
  1.24; development needs Go 1.25 because the `analysis` module
  (`golang.org/x/tools`) and golangci-lint 2.12.2 require it.
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

The repository holds several Go modules:

| Path | Module | Description |
|------|--------|-------------|
| `durable/` | root (published) | Core SDK: handler, context, checkpoint, futures, operations |
| `telemetry/` | `telemetry` (published) | Observability plugin, built on the experimental plugin API; versions below v1 |
| `analysis/` | `analysis` (published) | `durablelint` static analyzer for determinism rules; versions below v1 |
| `conformance/` | `conformance` (internal) | Conformance test handlers for cross-SDK validation |
| `examples/` | `examples` (internal) | Self-contained example Lambda functions |
| `.github/scripts/release/` | `release` (internal) | Release tool; see [RELEASING.md](RELEASING.md) |

Each module with its own `go.mod` is built and tested independently.

Published modules are the ones users add with `go get`. Internal modules
exist only for this repository's tests and examples.

### Nested modules and the root module

`telemetry/go.mod` depends on the root module in two ways at once:

```
require github.com/aws/aws-durable-execution-sdk-go v1.0.0

replace github.com/aws/aws-durable-execution-sdk-go => ..
```

The `replace` directive applies only when `telemetry` is the main module,
that is, when you build or test inside `telemetry/`. It points at the root
module in this checkout, so local development always compiles against the
current source with no extra step.

A user's build ignores that `replace` directive. It reads only the `require`
line, so the version there must be a released root module version. The
release tool's `prepare` command sets it when an SDK release is prepared;
do not edit it by hand. Between releases it names the most recent release.
So a change to `telemetry` that needs an unreleased root module API builds
in the repository, but users cannot build it until the next SDK release.
See [RELEASING.md](RELEASING.md).

`analysis` does not depend on the root module. The internal modules
(`conformance`, `examples`) have the same `require` and `replace` pair, and
`prepare` sets their requirement too: `examples` depends on both the root
module and `telemetry`, and Go requires it to record the same root version
as `telemetry`. Nobody consumes the internal modules from outside, so they
are never released.

The `consumable` job in CI builds a new module that imports every package
of the published nested modules, with every module taken from the
checkout. Run it locally with:

```bash
go run -C .github/scripts/release . consumer telemetry analysis
```

## Running Tests

Run tests with the race detector in each module:

```bash
go test -race ./...
```

Or use the Makefile target from the repository root (covers the root module):

```bash
make test
```

For the `telemetry/` module:

```bash
cd telemetry && go test -race ./...
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

Both run the following steps in each of the five modules (root/durable,
telemetry, conformance, examples, analysis) and in the release tool's module
(`.github/scripts/release`):

1. `go build ./...`
2. `go vet ./...`
3. `golangci-lint run ./...`
4. `golangci-lint fmt --diff ./...` (fails if any file is unformatted)
5. `go test -race ./...`

The examples module also runs `go vet -tags cloud ./cloud` to check the
cloud test harness compiles, and its `readme` package compiles every Go
code block of the root `README.md` against the checkout and runs the ones
that declare tests. The analysis module also builds `durablelint`
and runs it over the examples and conformance modules, which must report
nothing. See [analysis/README.md](analysis/README.md) for the analyzer.

With no module argument, the script also runs the release tool's `check`
and `consumer` commands, as the `release-tool` and `consumable` CI jobs do.

You can check a single module by passing its path:

```bash
sh scripts/ci-local.sh examples
```

The `supported-go` CI job also builds, vets, and tests the published
modules (root, `telemetry`, `analysis`) with the two newest Go releases
(`stable` and `oldstable` in `actions/setup-go`). The other jobs use the Go
version each `go.mod` declares, which is the oldest Go release that builds
the module.

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

## Releasing

[RELEASING.md](RELEASING.md) describes how to release the SDK, `telemetry`,
and `analysis`.

## Security issue notifications

If you discover a potential security issue in this project, we ask that you
notify AWS/Amazon Security via our
[vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/).
Please do **not** create a public GitHub issue.

## License

This project is licensed under the Apache License 2.0. By contributing, you
agree that your contributions will be licensed under the same terms. See the
[LICENSE](LICENSE) file for details.

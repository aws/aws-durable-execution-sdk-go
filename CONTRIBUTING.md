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

The repository holds several Go modules:

| Path | Module | Description |
|------|--------|-------------|
| `durable/` | root (published) | Core SDK: handler, context, checkpoint, futures, operations |
| `insight/` | `insight` (published) | Observability plugin |
| `analysis/` | `analysis` (published) | `durablelint` static analyzer for determinism rules |
| `conformance/` | `conformance` (internal) | Conformance test handlers for cross-SDK validation |
| `examples/` | `examples` (internal) | Self-contained example Lambda functions |

Each module with its own `go.mod` is built and tested independently.

Published modules are the ones users add with `go get`. Internal modules
exist only for this repository's tests and examples.

### Nested modules and the root module

`insight/go.mod` depends on the root module in two ways at once:

```
require github.com/aws/aws-durable-execution-sdk-go v0.1.0

replace github.com/aws/aws-durable-execution-sdk-go => ..
```

The `replace` directive applies only when `insight` is the main module,
that is, when you build or test inside `insight/`. It points at the root
module in this checkout, so local development always compiles against the
current source with no extra step.

A user's build ignores that `replace` directive. It reads only the `require`
line, so the version there must be a published root module tag. The release
script (below) rewrites it to the version being released; do not edit it by
hand. Between releases it names the most recent release, which is why a
change to `insight` that needs an unreleased root module API builds locally
but is not usable by external consumers until the next release.

`analysis` does not depend on the root module. The internal modules
(`conformance`, `examples`) have the same `require` and `replace` pair, and
the release script pins them too: `examples` depends on both the root
module and `insight`, and Go requires it to record the same root version
as `insight`. Nobody consumes the internal modules from outside, so they
get no tags.

The `consumable` job in CI builds a throwaway module that imports every
package of the published nested modules, with both the nested and the root
module taken from the checkout. Run it locally with:

```bash
sh scripts/verify-consumable.sh --root-from-checkout
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

Both run the following steps in each of the five modules (root/durable,
insight, conformance, examples, analysis):

1. `go build ./...`
2. `go vet ./...`
3. `golangci-lint run ./...`
4. `golangci-lint fmt --diff ./...` (fails if any file is unformatted)
5. `go test -race ./...`

The examples module also runs `go vet -tags cloud ./cloud` to check the
cloud test harness compiles. The analysis module also builds `durablelint`
and runs it over the examples and conformance modules, which must report
nothing. See [analysis/README.md](analysis/README.md) for the analyzer.

With no module argument, the script also runs the repository-level checks
of the `consumable` CI job: `scripts/release_test.sh` (tests of the
release script) and `scripts/verify-consumable.sh --root-from-checkout`.

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

## Releasing

The root module and the published nested modules (`insight`, `analysis`)
are released together at one version. A release is a set of Git tags; there
is no registry upload.

### Versioning and compatibility

- The root module is tagged `vX.Y.Z`.
- Each published nested module is tagged `<dir>/vX.Y.Z`, the form the Go
  toolchain expects for a module in a subdirectory: `insight/v0.2.0`,
  `analysis/v0.2.0`.
- All three tags of one release point at the same commit and carry the
  same version number.
- `insight/vX.Y.Z` declares a dependency on the root module `vX.Y.Z`, the
  version it was built and tested with. Go treats a declared version as a
  minimum: a consumer that also requires a newer root module gets the newer
  one. The plugin API the two share is stable within a major version, so a
  newer root module keeps working with an older `insight`; keep both at
  the same version anyway, because that is the pairing the release tested.
- `analysis/vX.Y.Z` has no dependency on the root module; its version
  only records which release it belongs to.

Before the first release, `insight/go.mod` pins a root version that does
not exist yet. Local builds are unaffected (they use the `replace`
directive); the module becomes usable by external consumers with the first
release.

### Procedure

1. Make sure `main` is green in CI and your checkout of it is clean and up
   to date.
2. Run the Release workflow in GitHub Actions (`.github/workflows/release.yml`)
   from `main` with the version to release, for example `v0.2.0` or
   `v0.2.0-beta.1`. The workflow refuses any other branch or tag chosen at
   dispatch time, and it pushes the release commit to `main` only.
   The workflow runs `scripts/release.sh`, which:
   1. rewrites the root module requirement in every nested module that has
      one (`insight`, `conformance`, `examples`) to the release version;
   2. commits that change as `chore(release): vX.Y.Z`, or commits nothing
      when the pins already match;
   3. creates the annotated tags `vX.Y.Z`, `insight/vX.Y.Z`, and
      `analysis/vX.Y.Z` on that commit.

   The workflow then pushes the commit and the root tag, builds a throwaway
   consumer of the nested modules against the now-published root module,
   and only then pushes the nested module tags. A last step fetches every
   published module from the module proxy with `go get` and builds it.
3. Write the release notes for the tag from `docs/release-notes.md`.

The script also runs locally, for a release from a maintainer's machine:

```bash
sh scripts/release.sh v0.2.0
git push origin main v0.2.0 insight/v0.2.0 analysis/v0.2.0
sh scripts/verify-consumable.sh --released v0.2.0
```

If `main` only accepts pull requests, open one with the `chore(release)`
commit, merge it, and run `scripts/release.sh` again on the merged commit.
The pins already match, so the second run creates only the tags.

The script refuses a dirty working tree and a `v2` or later version,
because a new major version changes every module path. It also refuses a
version whose tags point at a commit other than the one checked out: that
version is taken.

### Resuming a partial release

The workflow pushes the root tag before it verifies the nested modules. If
that verification fails, or the module proxy has not served the new tag
within the retry window, the root tag is published and the nested tags are
not. To finish the release, run the workflow again with the same version.
`main` still ends at the release commit, so the script keeps the tags that
already point there, creates the missing ones, and the workflow pushes only
those. The same holds locally: rerun `scripts/release.sh` with the same
version on the release commit and push the tags it lists.

The workflow can resume only while `main` ends at the release commit. Once
more commits land on `main`, check out the release commit locally
(`git checkout vX.Y.Z`), run `scripts/release.sh vX.Y.Z` there, and push
the tags it lists; or release the next version instead.

`scripts/release_test.sh` exercises the script against a fixture
repository and runs in the `consumable` CI job.

## License

This project is licensed under the Apache License 2.0. By contributing, you
agree that your contributions will be licensed under the same terms. See the
[LICENSE](LICENSE) file for details.

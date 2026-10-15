# Releasing

This document describes how to release the Go modules in this repository,
and what the Release workflow checks after a release.

Go modules are not uploaded to a registry. The go command fetches a
module version from the version's Git tag, through the module proxy at
`proxy.golang.org`. So a Go release is a tag. Publishing a GitHub Release
creates its tag, and from then on every user can fetch that version.

## Modules

| Module | Path | Tag |
| --- | --- | --- |
| `github.com/aws/aws-durable-execution-sdk-go` (the SDK) | `.` | `vX.Y.Z` |
| `github.com/aws/aws-durable-execution-sdk-go/insight` | `insight` | `insight/vX.Y.Z` |
| `github.com/aws/aws-durable-execution-sdk-go/analysis` | `analysis` | `analysis/vX.Y.Z` |

The go command requires the leading `v` of each version. The tag of a
module in a subdirectory starts with the directory, as in
`insight/v0.1.0` ([Go modules reference](https://go.dev/ref/mod#vcs-version)).
The `conformance` and `examples` modules exist only for this repository's
tests, so they are never released.

Each GitHub Release releases one module. To release two modules at the
same commit, create two GitHub Releases.

## Versioning

The README's [Versioning](README.md#versioning) section states what each
release promises to users. These rules follow from it:

- The SDK is at v1. A v2 would need a new module path, ending in `/v2`,
  which the release tool does not set up.
- insight and analysis stay below v1. The release tool refuses v1 or
  later for them.
- A version has the form `vX.Y.Z`, optionally followed by `-alpha`,
  `-beta`, or `-rc` and an optional `.N`, as in `v1.2.0` and
  `v1.2.0-rc.1`. The release tool refuses other forms, such as
  `v1.2.0-rc1` and `v1.2.0-preview`.
- `durable.Version`, in `durable/version.go`, holds the SDK version
  without the leading `v`, as `1.2.0`. The default client sends it in the
  User-Agent header of every Lambda API call, as
  `aws-durable-execution-sdk-go/1.2.0`. That is the token format of the
  durable execution SDKs for the other languages.
- insight, conformance, and examples require the SDK. Each must require
  the SDK version that `durable.Version` names. Their `replace` directives
  point at this checkout, so builds inside the repository use the current
  SDK source. A user's build ignores those directives and reads only the
  `require` line. So an insight release requires the SDK version it was
  released with.

A published version cannot change. The module proxy keeps the first copy
of each version that it serves, and the checksum database records the
hash of that copy. A user who fetches a different copy, for example from
a tag that was moved, gets a checksum error. So fix a mistake by
releasing the next version. Never move a tag or create it again. To keep
users off a broken version, add a
[`retract` directive](https://go.dev/ref/mod#go-mod-file-retract) for it
to the module's `go.mod` in the next release.

Before a minor or patch release of the SDK, check for incompatible API
changes. `gorelease` lists the API differences from a released version.
It needs Go 1.26 or later.

```console
go run golang.org/x/exp/cmd/gorelease@latest -base=v1.0.0
```

`gorelease` reports a change to the experimental plugin API as
incompatible. The compatibility promise allows such a change in a minor
release. Any other incompatible change needs a new major version.

## Releasing the SDK

### 1. Set the version

From the repository root, run the release tool's `prepare` command with
the new version:

```console
go run -C .github/scripts/release . prepare v1.1.0
```

The command sets `durable.Version` to `1.1.0`. It also sets the SDK
version that insight, conformance, and examples require to `v1.1.0`. In
`docs/release-notes.md`, put the changes since the last release under a
`## v1.1.0` heading. Commit the changes as `chore(release): v1.1.0`, open
a pull request, and merge it to `main`. CI runs on the pull request, so
it checks the User-Agent header with the new version.

Create the release soon after the merge. Until the `v1.1.0` tag exists,
insight at `main` requires an SDK version that does not exist.

### 2. Create the GitHub Release

1. Check out the commit you will tag, and run:

   ```console
   go run -C .github/scripts/release . check v1.1.0
   ```

2. On the [Releases page](https://github.com/aws/aws-durable-execution-sdk-go/releases),
   click **Draft a new release**.
3. Under **Choose a tag**, type `v1.1.0`, and choose to create the tag on
   publish. Set the target to `main`, or to the commit you checked if
   other commits merged after it.
4. Set the title to the tag.
5. Write the release notes. See [Release notes](#release-notes).
6. For an `-alpha`, `-beta`, or `-rc` version, check
   **Set as a pre-release**.
7. Click **Publish release**.

With the GitHub CLI, the same release is:

```console
gh release create v1.1.0 --target <commit> --title v1.1.0 --notes-file notes.md
```

## Releasing insight or analysis

insight and analysis hold their versions only in their tags. So their
releases need no version commit. Each gets a GitHub Release of its own,
tagged `insight/vX.Y.Z` or `analysis/vX.Y.Z`, on a commit on `main`.

insight requires the SDK version that `durable.Version` names at the
tagged commit. A user's build fetches that SDK version from its release,
so the SDK version must be released first. Before you create an insight
release, check out the commit and run:

```console
go run -C .github/scripts/release . check insight/v0.2.0
```

The command builds insight from the checkout against the released SDK
version that insight requires. It fails when insight uses SDK code that
is not released yet. The go command fetches the SDK version as it is
configured to, through `GOPROXY` or from the module cache.

Then create the GitHub Release as for the SDK, with the tag
`insight/v0.2.0`. Clear **Set as the latest release**, so that the
repository's latest release stays an SDK release. With the GitHub CLI,
add `--latest=false`.

To release the SDK and insight together, release the SDK first. When its
Release workflow passes, release insight at the same commit.

Go treats a required version as a minimum. So a user who upgrades the SDK
keeps the insight version they had. Release a new insight version after
each SDK release that changes the plugin API.

## How the Release workflow works

Publishing a GitHub Release starts the
[Release workflow](.github/workflows/release.yml). Its token can only
read the repository. The workflow runs the release tool's `verify`
command on the release's tag. The command checks, in this order:

1. The tag points at a commit on `main`.
2. The tagged commit's `durable.Version` and the SDK version that each
   nested module requires agree. For an SDK release, they name the
   released version.
3. The module proxy serves the version. When the proxy reports the commit
   it fetched the version from, that commit must be the tagged one. A new
   version normally appears within a minute. If someone asked the proxy
   for the version before the tag existed, the proxy can answer that the
   version does not exist for up to 30 minutes
   ([proxy.golang.org FAQ](https://proxy.golang.org/)). So the command
   retries for up to 35 minutes. Asking the proxy for the version also
   makes [pkg.go.dev](https://pkg.go.dev/about#adding-a-package) list it.
4. A new module that imports every package of the released module
   builds, with every module fetched from the module proxy into an empty
   module cache. This is the build that a new user runs.

The workflow changes nothing. So when it fails, the release is already
published with the problem that it reports. If the proxy did not serve
the version in time, run the workflow again. Otherwise, fix the problem
and release the next version.

## Release notes

`docs/release-notes.md` records the changes under a heading for each SDK
release. Write the notes of a GitHub Release from those entries. Each
GitHub Release covers one module, so its notes list only that module's
changes. Name every change to the experimental plugin API, because a
minor release may break it.

## Checklist

Before you publish a release:

- [ ] For an SDK release, the `chore(release)` pull request is merged to `main`.
- [ ] CI passes on `main`.
- [ ] `check <tag>` passes on the commit you will tag.
- [ ] The tag is `vX.Y.Z`, `insight/vX.Y.Z`, or `analysis/vX.Y.Z`, and targets that commit.
- [ ] The release notes are written.
- [ ] **Set as a pre-release** is checked for an `-alpha`, `-beta`, or `-rc` version.
- [ ] **Set as the latest release** is cleared for an insight or analysis release.

After you publish the release, check that the Release workflow passes.

## The release tool

The release tool is the Go program in
[`.github/scripts/release`](.github/scripts/release). It is a module of
its own, so no published module contains it, and it uses only the Go
standard library. Run it from the repository root as
`go run -C .github/scripts/release . <command>`.

| Command | What it does |
| --- | --- |
| `prepare vX.Y.Z` | Sets `durable.Version`, and the SDK version that each nested module requires, for an SDK release. |
| `check [tag]` | Checks that `durable.Version` and the SDK version that each nested module requires agree. With a tag, also checks that the checked-out commit is ready for that release. |
| `consumer [dir...]` | Builds a new module that imports every package of each published module, with every module taken from the checkout. |
| `verify tag` | Checks a published release. The Release workflow runs it. |

The `release-tool` CI job runs the tool's tests and `check`. The
`consumable` CI job runs `consumer` for insight and analysis. That
catches a nested module that builds only through its own `replace`
directives.

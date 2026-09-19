#!/bin/sh
# Tests for scripts/release.sh. Builds a small repository in a temporary
# directory with the same module layout as this one (root module, published
# nested modules insight and analysis, internal modules conformance and
# examples) and checks what the release script does to it. Needs git and
# go; no network.
#
# Usage: scripts/release_test.sh

set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT_MODULE=github.com/aws/aws-durable-execution-sdk-go

WORK=$(mktemp -d -t release-test.XXXXXX)
trap 'rm -rf "$WORK"' EXIT
REPO="$WORK/repo"

export GOWORK=off
export GOFLAGS=-mod=mod
export GIT_AUTHOR_NAME=test GIT_AUTHOR_EMAIL=test@example.com
export GIT_COMMITTER_NAME=test GIT_COMMITTER_EMAIL=test@example.com

failures=0
pass() { echo "ok   - $1"; }
fail() { echo "FAIL - $1" >&2; failures=$((failures + 1)); }
check() { # check <description> <command...>
    desc=$1; shift
    if "$@" >/dev/null 2>&1; then pass "$desc"; else fail "$desc"; fi
}
check_not() {
    desc=$1; shift
    if "$@" >/dev/null 2>&1; then fail "$desc"; else pass "$desc"; fi
}

# nested <dir> <requires-root: yes|no>
nested() {
    mkdir -p "$REPO/$1"
    {
        echo "module $ROOT_MODULE/$1"
        echo
        echo "go 1.24"
        if [ "$2" = yes ]; then
            echo
            echo "require $ROOT_MODULE v0.0.0"
            echo
            echo "replace $ROOT_MODULE => .."
        fi
    } > "$REPO/$1/go.mod"
    if [ "$2" = yes ]; then
        printf 'package %s\n\nimport _ "%s/durable"\n' "$1" "$ROOT_MODULE" > "$REPO/$1/$1.go"
    else
        printf 'package %s\n' "$1" > "$REPO/$1/$1.go"
    fi
}

# Fixture repository.
mkdir -p "$REPO/durable" "$REPO/scripts"
printf 'module %s\n\ngo 1.24\n' "$ROOT_MODULE" > "$REPO/go.mod"
printf 'package durable\n' > "$REPO/durable/durable.go"
nested insight yes
nested analysis no
nested conformance yes
nested examples yes
# examples also depends on insight, as in this repository. Go requires it to
# record the same root module version as insight, so the release must pin
# examples as well or `go build` in examples fails.
{
    echo
    echo "require $ROOT_MODULE/insight v0.0.0"
    echo
    echo "replace $ROOT_MODULE/insight => ../insight"
} >> "$REPO/examples/go.mod"
printf 'package examples\n\nimport _ "%s/insight"\n' "$ROOT_MODULE" > "$REPO/examples/insight.go"
cp "$HERE/release.sh" "$REPO/scripts/release.sh"
git -C "$REPO" init -q -b main
git -C "$REPO" add -A
git -C "$REPO" commit -q -m "initial"

release() { (cd "$REPO" && sh scripts/release.sh "$@"); }
requires() { # requires <dir> <version>
    go mod edit -json "$REPO/$1/go.mod" | tr -d ' \t\n' | grep -Fq "\"Path\":\"$ROOT_MODULE\",\"Version\":\"$2\""
}
tag_at_head() { [ "$(git -C "$REPO" rev-parse "$1^{commit}")" = "$(git -C "$REPO" rev-parse HEAD)" ]; }

# Argument validation happens before anything changes.
check_not "rejects a version without the v prefix" release 0.3.0
check_not "rejects a non-semver version" release v0.3
check_not "rejects a v2 release (needs a module path suffix)" release v2.0.0
check_not "rejects no arguments" release
check "invalid arguments leave the tree unchanged" test -z "$(git -C "$REPO" status --porcelain)"
check "invalid arguments create no tags" test -z "$(git -C "$REPO" tag -l)"

# A dirty tree is refused.
echo "// scratch" >> "$REPO/durable/durable.go"
check_not "refuses a dirty working tree" release v0.3.0
git -C "$REPO" checkout -q -- durable/durable.go

# A normal release.
check "releases v0.3.0" release v0.3.0
check "insight pins the root module to v0.3.0" requires insight v0.3.0
check "insight keeps its local replace directive" grep -q "^replace $ROOT_MODULE => \.\.$" "$REPO/insight/go.mod"
check "examples (internal) pins the root module to v0.3.0" requires examples v0.3.0
check "conformance (internal) pins the root module to v0.3.0" requires conformance v0.3.0
check "examples keeps its insight requirement at v0.0.0" grep -q "^require $ROOT_MODULE/insight v0.0.0$" "$REPO/examples/go.mod"
check "examples still builds" sh -c "cd '$REPO/examples' && go build ./..."
check "analysis (no root dependency) is unchanged" test -z "$(git -C "$REPO" diff HEAD~1 -- analysis)"
check "commits the pins as chore(release)" test "$(git -C "$REPO" log -1 --format=%s)" = "chore(release): v0.3.0"
check "leaves the tree clean" test -z "$(git -C "$REPO" status --porcelain)"
check "tags the root module v0.3.0 at HEAD" tag_at_head v0.3.0
check "tags insight/v0.3.0 at HEAD" tag_at_head insight/v0.3.0
check "tags analysis/v0.3.0 at HEAD" tag_at_head analysis/v0.3.0
check_not "creates no examples tag" git -C "$REPO" rev-parse -q --verify refs/tags/examples/v0.3.0
check_not "creates no conformance tag" git -C "$REPO" rev-parse -q --verify refs/tags/conformance/v0.3.0
check "tags are annotated" test "$(git -C "$REPO" cat-file -t insight/v0.3.0)" = tag

# Re-running the same version at the release commit resumes: tags that
# exist at HEAD are kept and the missing ones are added. This is the path
# for a release that stopped after some tags were pushed.
released_at=$(git -C "$REPO" rev-parse HEAD)
git -C "$REPO" tag -d insight/v0.3.0 analysis/v0.3.0 >/dev/null
check "resumes v0.3.0 when the root tag exists at HEAD" release v0.3.0
check "resuming adds no commit" test "$(git -C "$REPO" rev-parse HEAD)" = "$released_at"
check "resuming keeps the root tag" tag_at_head v0.3.0
check "resuming recreates insight/v0.3.0 at HEAD" tag_at_head insight/v0.3.0
check "resuming recreates analysis/v0.3.0 at HEAD" tag_at_head analysis/v0.3.0
check "resuming with every tag present succeeds" release v0.3.0
check "resuming with every tag present adds no commit" test "$(git -C "$REPO" rev-parse HEAD)" = "$released_at"

# A version whose tags point at another commit is taken.
echo "// later" >> "$REPO/durable/durable.go"
git -C "$REPO" commit -q -am "later work"
check_not "refuses a version tagged at another commit" release v0.3.0
check "refusal leaves the tree unchanged" test -z "$(git -C "$REPO" status --porcelain)"
check "refusal moves no tag" test "$(git -C "$REPO" rev-parse 'v0.3.0^{commit}')" = "$released_at"
git -C "$REPO" reset -q --hard "$released_at"

# A tag at HEAD whose pins do not match the version is refused before any
# edit: a new pin commit would move HEAD away from that tag.
git -C "$REPO" tag -a v0.9.0 -m v0.9.0
check_not "refuses a tag at HEAD when the pins differ" release v0.9.0
check "the pin refusal leaves the tree unchanged" test -z "$(git -C "$REPO" status --porcelain)"
check_not "the pin refusal creates no nested tag" git -C "$REPO" rev-parse -q --verify refs/tags/insight/v0.9.0
git -C "$REPO" tag -d v0.9.0 >/dev/null

# Resuming on a checked-out release commit (detached HEAD) works and prints
# a push command without a branch.
git -C "$REPO" tag -d insight/v0.3.0 >/dev/null
git -C "$REPO" checkout -q --detach v0.3.0
check "resumes on a detached release commit" release v0.3.0
check "detached resume recreates insight/v0.3.0" tag_at_head insight/v0.3.0
check "detached resume prints a tags-only push" sh -c "(cd '$REPO' && sh scripts/release.sh v0.3.0) | grep -q '^  git push origin  v0.3.0 insight/v0.3.0 analysis/v0.3.0$'"
git -C "$REPO" checkout -q main

# Pins already at the version: tag without a new commit. This is the path
# for tagging a merged release commit.
git -C "$REPO" tag -d v0.3.0 insight/v0.3.0 analysis/v0.3.0 >/dev/null
before=$(git -C "$REPO" rev-parse HEAD)
check "re-releases v0.3.0 when the pins already match" release v0.3.0
check "adds no commit when the pins already match" test "$(git -C "$REPO" rev-parse HEAD)" = "$before"
check "still tags insight/v0.3.0" tag_at_head insight/v0.3.0

# A later release moves the pin forward, including a pre-release version.
check "releases v0.4.0-beta.1" release v0.4.0-beta.1
check "insight pins the root module to v0.4.0-beta.1" requires insight v0.4.0-beta.1
check "tags insight/v0.4.0-beta.1 at HEAD" tag_at_head insight/v0.4.0-beta.1
check "v0.3.0 tags stay where they were" test "$(git -C "$REPO" rev-parse 'v0.3.0^{commit}')" = "$before"

if [ "$failures" -ne 0 ]; then
    echo "$failures check(s) failed" >&2
    exit 1
fi
echo "All release checks passed."

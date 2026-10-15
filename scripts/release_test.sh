#!/bin/sh
# Tests for scripts/release.sh. Builds a small repository in a temporary
# directory with the same module layout as this one (root module with a
# Version constant, published nested modules insight and analysis, internal
# modules conformance and examples) and checks what the release script does
# to it. Needs git and go; no network.
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

# version_file <version> writes the root module's Version constant.
version_file() {
    printf 'package durable\n\n// Version is the module version.\nconst Version = "%s"\n' "$1" > "$REPO/durable/version.go"
}

# Fixture repository.
mkdir -p "$REPO/durable" "$REPO/scripts"
printf 'module %s\n\ngo 1.24\n' "$ROOT_MODULE" > "$REPO/go.mod"
printf 'package durable\n' > "$REPO/durable/durable.go"
version_file 0.1.0
# The release script runs TestVersionFormat before it commits. The fixture's
# version of the test fails for Version 0.9.9, which stands in for a
# version whose User-Agent tests fail.
cat > "$REPO/durable/version_test.go" <<'EOF'
package durable

import "testing"

func TestVersionFormat(t *testing.T) {
	if Version == "0.9.9" {
		t.Fatal("this version fails the User-Agent tests")
	}
}
EOF
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
version_is() { grep -q "^const Version = \"$1\"\$" "$REPO/durable/version.go"; }
tag_at_head() { [ "$(git -C "$REPO" rev-parse "$1^{commit}")" = "$(git -C "$REPO" rev-parse HEAD)" ]; }
no_tag() { ! git -C "$REPO" rev-parse -q --verify "refs/tags/$1" >/dev/null; }
clean() { test -z "$(git -C "$REPO" status --porcelain)"; }

# Argument validation happens before anything changes.
check_not "rejects no arguments" release
check_not "rejects a version without the v prefix" release 1.0.0
check_not "rejects a non-semver version" release v1.0
check_not "rejects a pre-release label other than alpha, beta, or rc" release v1.0.0-preview.1
check_not "rejects a pre-release number without a dot" release v1.0.0-rc1
check_not "rejects build metadata" release v1.0.0+build.1
check_not "rejects a leading zero" release v1.02.0
check_not "rejects a v2 release (needs a module path suffix)" release v2.0.0
check_not "rejects insight at v1 (it stays below v1)" release v1.0.0 insight/v1.0.0
check_not "rejects analysis at v1 (it stays below v1)" release v1.0.0 analysis/v1.0.0
check_not "rejects an invalid insight version" release v1.0.0 insight/v0.1
check_not "rejects a module version without the directory" release v1.0.0 v0.1.0
check_not "rejects an unknown module" release v1.0.0 nosuch/v0.1.0
check_not "rejects an internal module" release v1.0.0 examples/v0.1.0
check_not "rejects the insight version given twice" release v1.0.0 insight/v0.1.0 insight/v0.2.0
check "invalid arguments leave the tree unchanged" clean
check "invalid arguments create no tags" test -z "$(git -C "$REPO" tag -l)"

# A dirty tree is refused.
echo "// scratch" >> "$REPO/durable/durable.go"
check_not "refuses a dirty working tree" release v1.0.0
git -C "$REPO" checkout -q -- durable/durable.go

# A Version constant the script cannot find is refused before any edit.
printf 'package durable\n\nconst (\n\tVersion = "0.1.0"\n)\n' > "$REPO/durable/version.go"
git -C "$REPO" commit -q -am "unexpected version file"
check_not "refuses a version file without a const Version line" release v1.0.0
check "that refusal leaves the tree unchanged" clean
check "that refusal creates no tags" test -z "$(git -C "$REPO" tag -l)"
git -C "$REPO" reset -q --hard HEAD~1

# A failing User-Agent test stops the release before the commit, so no
# commit and no tag exist. Only the Version constant is edited, and the
# refusal says how to restore it.
initial=$(git -C "$REPO" rev-parse HEAD)
if release v0.9.9 > "$WORK/refusal.log" 2>&1; then
    fail "refuses a release whose User-Agent tests fail"
else
    pass "refuses a release whose User-Agent tests fail"
fi
check "that refusal adds no commit" test "$(git -C "$REPO" rev-parse HEAD)" = "$initial"
check "that refusal creates no tags" test -z "$(git -C "$REPO" tag -l)"
check "that refusal edits only the version file" test "$(git -C "$REPO" status --porcelain)" = " M durable/version.go"
check "that refusal names the restore command" grep -q 'git checkout -- durable/version.go' "$WORK/refusal.log"
git -C "$REPO" checkout -q -- durable/version.go

# A release of the root module alone.
check "releases v1.0.0 alone" release v1.0.0
check "sets the Version constant to 1.0.0" version_is 1.0.0
check "keeps the rest of the version file" grep -q '^// Version is the module version\.$' "$REPO/durable/version.go"
check "insight pins the root module to v1.0.0" requires insight v1.0.0
check "insight keeps its local replace directive" grep -q "^replace $ROOT_MODULE => \.\.$" "$REPO/insight/go.mod"
check "examples (internal) pins the root module to v1.0.0" requires examples v1.0.0
check "conformance (internal) pins the root module to v1.0.0" requires conformance v1.0.0
check "examples keeps its insight requirement at v0.0.0" grep -q "^require $ROOT_MODULE/insight v0.0.0$" "$REPO/examples/go.mod"
check "examples still builds" sh -c "cd '$REPO/examples' && go build ./..."
check "analysis (no root dependency) is unchanged" test -z "$(git -C "$REPO" diff HEAD~1 -- analysis)"
check "commits the release as chore(release)" test "$(git -C "$REPO" log -1 --format=%s)" = "chore(release): v1.0.0"
check "the release commit holds the Version constant" sh -c "git -C '$REPO' diff --name-only HEAD~1 HEAD | grep -qx durable/version.go"
check "leaves the tree clean" clean
check "tags the root module v1.0.0 at HEAD" tag_at_head v1.0.0
check "the root tag is annotated" test "$(git -C "$REPO" cat-file -t v1.0.0)" = tag
check "creates no nested module tags" test -z "$(git -C "$REPO" tag -l '*/*')"

# A release that also tags both nested modules, each at its own version.
check "releases v1.1.0 with insight/v0.1.0 and analysis/v0.1.0" release v1.1.0 insight/v0.1.0 analysis/v0.1.0
check "sets the Version constant to 1.1.0" version_is 1.1.0
check "insight pins the root module to v1.1.0" requires insight v1.1.0
check "tags the root module v1.1.0 at HEAD" tag_at_head v1.1.0
check "tags insight/v0.1.0 at HEAD" tag_at_head insight/v0.1.0
check "tags analysis/v0.1.0 at HEAD" tag_at_head analysis/v0.1.0
check "creates no insight tag at the root version" no_tag insight/v1.1.0
check "creates no analysis tag at the root version" no_tag analysis/v1.1.0
check "creates no examples tag" no_tag examples/v1.1.0
check "creates no conformance tag" no_tag conformance/v1.1.0
check "nested tags are annotated" test "$(git -C "$REPO" cat-file -t insight/v0.1.0)" = tag
released_at=$(git -C "$REPO" rev-parse HEAD)

# Re-running the same versions at the release commit resumes: tags that
# exist at HEAD are kept and the missing ones are added. This is the path
# for a release that stopped after some tags were pushed.
git -C "$REPO" tag -d insight/v0.1.0 analysis/v0.1.0 >/dev/null
check "resumes v1.1.0 when the root tag exists at HEAD" release v1.1.0 insight/v0.1.0 analysis/v0.1.0
check "resuming adds no commit" test "$(git -C "$REPO" rev-parse HEAD)" = "$released_at"
check "resuming keeps the root tag" tag_at_head v1.1.0
check "resuming recreates insight/v0.1.0 at HEAD" tag_at_head insight/v0.1.0
check "resuming recreates analysis/v0.1.0 at HEAD" tag_at_head analysis/v0.1.0
check "resuming with every tag present succeeds" release v1.1.0 insight/v0.1.0 analysis/v0.1.0
check "resuming with every tag present adds no commit" test "$(git -C "$REPO" rev-parse HEAD)" = "$released_at"

# A version whose tag points at another commit is taken, for the root
# module and for a nested module alike.
echo "// later" >> "$REPO/durable/durable.go"
git -C "$REPO" commit -q -am "later work"
check_not "refuses a root version tagged at another commit" release v1.1.0
check_not "refuses an insight version tagged at another commit" release v1.1.1 insight/v0.1.0
check_not "refuses an analysis version tagged at another commit" release v1.1.1 analysis/v0.1.0
check "the refusals leave the tree unchanged" clean
check "the refusals create no root tag" no_tag v1.1.1
check "the refusals move no tag" test "$(git -C "$REPO" rev-parse 'v1.1.0^{commit}')" = "$released_at"
git -C "$REPO" reset -q --hard "$released_at"

# A tag at HEAD whose release is not in the tree is refused before any
# edit: a new release commit would move HEAD away from that tag. First the
# pins differ, then only the Version constant.
git -C "$REPO" tag -a v0.9.0 -m v0.9.0
check_not "refuses a tag at HEAD when the pins differ" release v0.9.0 insight/v0.0.9
check "the pin refusal leaves the tree unchanged" clean
check "the pin refusal creates no nested tag" no_tag insight/v0.0.9
git -C "$REPO" tag -d v0.9.0 >/dev/null
git -C "$REPO" tag -d v1.1.0 insight/v0.1.0 analysis/v0.1.0 >/dev/null
version_file 1.1.0-rc.1
git -C "$REPO" commit -q -am "change the Version constant"
git -C "$REPO" tag -a v1.1.0 -m v1.1.0
check_not "refuses a tag at HEAD when the Version constant differs" release v1.1.0 insight/v0.1.0
check "the constant refusal leaves the tree unchanged" clean
check "the constant refusal creates no nested tag" no_tag insight/v0.1.0
git -C "$REPO" tag -d v1.1.0 >/dev/null
git -C "$REPO" reset -q --hard "$released_at"

# The Version constant and the pins already at the version: tag without a
# new commit. This is the path for tagging a merged release commit.
check "re-releases v1.1.0 when the release is already in the tree" release v1.1.0 insight/v0.1.0 analysis/v0.1.0
check "adds no commit when the release is already in the tree" test "$(git -C "$REPO" rev-parse HEAD)" = "$released_at"
check "still tags v1.1.0" tag_at_head v1.1.0
check "still tags insight/v0.1.0" tag_at_head insight/v0.1.0
check "still tags analysis/v0.1.0" tag_at_head analysis/v0.1.0

# Resuming on a checked-out release commit (detached HEAD) works and prints
# a push command without a branch.
git -C "$REPO" tag -d insight/v0.1.0 >/dev/null
git -C "$REPO" checkout -q --detach v1.1.0
check "resumes on a detached release commit" release v1.1.0 insight/v0.1.0 analysis/v0.1.0
check "detached resume recreates insight/v0.1.0" tag_at_head insight/v0.1.0
check "detached resume prints a tags-only push" sh -c "(cd '$REPO' && sh scripts/release.sh v1.1.0 insight/v0.1.0 analysis/v0.1.0) | grep -q '^  git push origin  v1.1.0 insight/v0.1.0 analysis/v0.1.0$'"
git -C "$REPO" checkout -q main

# A later pre-release that tags insight and not analysis.
check "releases v1.2.0-rc.1 with insight/v0.2.0-beta.1 only" release v1.2.0-rc.1 insight/v0.2.0-beta.1
check "sets the Version constant to 1.2.0-rc.1" version_is 1.2.0-rc.1
check "insight pins the root module to v1.2.0-rc.1" requires insight v1.2.0-rc.1
check "tags v1.2.0-rc.1 at HEAD" tag_at_head v1.2.0-rc.1
check "tags insight/v0.2.0-beta.1 at HEAD" tag_at_head insight/v0.2.0-beta.1
check "tags no analysis version at that commit" test -z "$(git -C "$REPO" tag --points-at HEAD -l 'analysis/*')"
check "earlier tags stay where they were" test "$(git -C "$REPO" rev-parse 'insight/v0.1.0^{commit}')" = "$released_at"

if [ "$failures" -ne 0 ]; then
    echo "$failures check(s) failed" >&2
    exit 1
fi
echo "All release checks passed."

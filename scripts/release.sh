#!/bin/sh
# Prepare a release of the root module, and of the published nested
# modules whose versions are given.
#
# Usage:
#   scripts/release.sh vX.Y.Z [insight/vX.Y.Z] [analysis/vX.Y.Z]
#
# The first argument is the version of the root module, the SDK. The SDK is
# the only module at v1. The published nested modules, insight and
# analysis, each have a version of their own, below v1. A release tags a
# nested module only when its version is given, as insight/vX.Y.Z or
# analysis/vX.Y.Z.
#
# Each version has the form vX.Y.Z, optionally followed by -alpha, -beta,
# or -rc and an optional .N, for example v1.2.0 or v1.2.0-rc.1.
#
# The script:
#
#   1. Sets the Version constant in durable/version.go to the root module
#      version without the leading v. The default client sends that
#      constant in the User-Agent header of every Lambda API call. The
#      script then builds the root module and runs the tests that check
#      the constant and the header.
#   2. Pins every nested module that depends on the root module to the
#      root module version, so that an external consumer of a published
#      module resolves a root version that exists. The local `replace`
#      directive stays in place: consumers ignore it, and it keeps
#      in-repository development building against the checked-out root
#      module. Internal modules are pinned too, because a module that
#      depends on both the root module and a published nested module must
#      record the same root version as that nested module. The pins change
#      whether or not the release tags the nested modules.
#   3. Commits those changes as "chore(release): vX.Y.Z". It commits
#      nothing when the Version constant and the pins already match, so the
#      script can be re-run on a merged commit.
#   4. Creates the tags the Go toolchain expects: vX.Y.Z for the root
#      module and <dir>/vX.Y.Z for each nested module whose version is
#      given. A tag that already points at the release commit is kept, so
#      the script can resume a release that stopped after some tags were
#      pushed. A tag that points anywhere else is an error: the version is
#      taken.
#
# Nothing is pushed. The script prints the push command to run afterwards.
# See CONTRIBUTING.md, "Releasing", for the full procedure.
#
# Requires a clean working tree.

set -eu

ROOT=$(cd "$(dirname "$0")/.." && pwd)

# Published nested modules, as directories relative to ROOT. They stay
# below v1. Each has a version of its own, given on the command line as
# <dir>/vX.Y.Z, and gets a tag only when its version is given.
V0_MODULES="insight analysis"

# Nested modules used only inside the repository. They are pinned like the
# published ones but not tagged.
INTERNAL_MODULES="conformance examples"

# The file that declares the Version constant, relative to ROOT.
VERSION_FILE=durable/version.go

usage() {
    echo "usage: $0 vX.Y.Z [insight/vX.Y.Z] [analysis/vX.Y.Z]" >&2
    exit 2
}

fail() {
    echo "release: $*" >&2
    exit 1
}

# valid_version succeeds when $1 is a version this script releases. Numbers
# have no leading zeros, because Go ignores a tag such as v1.02.0.
valid_version() {
    printf '%s\n' "$1" | grep -Eq '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-(alpha|beta|rc)(\.(0|[1-9][0-9]*))?)?$'
}

VERSION_FORM="vX.Y.Z, optionally followed by -alpha, -beta, or -rc and an optional .N"

# major prints the major version number of the version $1.
major() {
    printf '%s\n' "$1" | sed -E 's/^v([0-9]+)\..*/\1/'
}

[ $# -ge 1 ] || usage
VERSION=$1
shift

valid_version "$VERSION" || fail "version must look like $VERSION_FORM, got '$VERSION'"

# A v2+ release changes every module path (a /v2 suffix), which this script
# does not do.
MAJOR=$(major "$VERSION")
[ "$MAJOR" -lt 2 ] || fail "major version $MAJOR needs a module path suffix; this script handles v0 and v1 only"

# The <dir>/vX.Y.Z arguments, checked and collected as a space-separated
# list.
OWN_VERSIONS=
for arg in "$@"; do
    case "$arg" in
        */*) ;;
        *) fail "expected <dir>/vX.Y.Z, got '$arg'" ;;
    esac
    mod=${arg%%/*}
    ver=${arg#*/}
    case " $V0_MODULES " in
        *" $mod "*) ;;
        *) fail "'$mod' is not a published nested module; these are: $V0_MODULES" ;;
    esac
    case " $OWN_VERSIONS " in
        *" $mod/"*) fail "the version of $mod is given more than once" ;;
    esac
    valid_version "$ver" || fail "the version of $mod must look like $VERSION_FORM, got '$ver'"
    [ "$(major "$ver")" -eq 0 ] || fail "$mod stays below v1, got '$ver'"
    OWN_VERSIONS="$OWN_VERSIONS $arg"
done

# own_version prints the version given for the module $1, and fails when
# none was given.
own_version() {
    for pair in $OWN_VERSIONS; do
        if [ "${pair%%/*}" = "$1" ]; then
            printf '%s\n' "${pair#*/}"
            return 0
        fi
    done
    return 1
}

cd "$ROOT"

[ -z "$(git status --porcelain)" ] || fail "working tree is not clean; commit or stash first"

ROOT_MODULE=$(sed -n 's/^module[[:space:]]*//p' go.mod)
[ -n "$ROOT_MODULE" ] || fail "cannot read module path from go.mod"

TAGS="$VERSION"
for mod in $V0_MODULES; do
    [ -f "$mod/go.mod" ] || fail "published module '$mod' has no go.mod"
    if ver=$(own_version "$mod"); then
        TAGS="$TAGS $mod/$ver"
    fi
done

# Tags that already exist must all point at HEAD. Then this run resumes a
# release whose remaining tags were never created or pushed, and HEAD is the
# release commit. A tag anywhere else means the version is already used.
HEAD_COMMIT=$(git rev-parse HEAD)
EXISTING_TAGS=
for tag in $TAGS; do
    if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
        at=$(git rev-parse "refs/tags/$tag^{commit}")
        [ "$at" = "$HEAD_COMMIT" ] || fail "tag $tag already exists at $(git rev-parse --short "$at"), not at HEAD"
        EXISTING_TAGS="$EXISTING_TAGS $tag"
    fi
done

# Does the tree already hold this release: the Version constant and the
# pins of the nested modules that depend on the root module? Decided before
# anything is edited, so that a refusal leaves the tree untouched.
CONST_VERSION=${VERSION#v}
[ "$(grep -c '^const Version = "[^"]*"$' "$VERSION_FILE" 2>/dev/null)" = 1 ] ||
    fail "$VERSION_FILE must declare the version on exactly one line of the form: const Version = \"X.Y.Z\""
CURRENT_CONST=$(sed -n 's/^const Version = "\([^"]*\)"$/\1/p' "$VERSION_FILE")
RELEASE_IN_TREE=yes
[ "$CURRENT_CONST" = "$CONST_VERSION" ] || RELEASE_IN_TREE=no

PINNED_MODULES=
for mod in $V0_MODULES $INTERNAL_MODULES; do
    [ -f "$mod/go.mod" ] || fail "nested module '$mod' has no go.mod"
    current=$(go mod edit -json "$mod/go.mod" | tr -d ' \t\n' \
        | sed -n "s|.*\"Path\":\"$ROOT_MODULE\",\"Version\":\"\([^\"]*\)\".*|\1|p")
    if [ -n "$current" ]; then
        PINNED_MODULES="$PINNED_MODULES $mod"
        [ "$current" = "$VERSION" ] || RELEASE_IN_TREE=no
    fi
done

# Resuming needs the release in the tree: a new release commit would move
# HEAD away from the tags that already exist.
if [ -n "$EXISTING_TAGS" ] && [ "$RELEASE_IN_TREE" = no ]; then
    fail "tag(s)$EXISTING_TAGS exist at HEAD, but the Version constant or the nested module pins do not match $VERSION"
fi

# Step 1: set the Version constant. The replacement is safe in sed, because
# valid_version admits only digits, letters, dots, and hyphens.
if [ "$CURRENT_CONST" != "$CONST_VERSION" ]; then
    echo "==> $VERSION_FILE: Version = \"$CONST_VERSION\""
    tmp=$(mktemp)
    sed "s/^const Version = \"[^\"]*\"\$/const Version = \"$CONST_VERSION\"/" "$VERSION_FILE" > "$tmp"
    cat "$tmp" > "$VERSION_FILE"
    rm -f "$tmp"
fi
# The default client sends Version in the User-Agent header. These tests
# check the constant and the header a request carries, so a release whose
# header would be wrong gets no commit and no tags.
echo "==> root module: go build ./... and the User-Agent tests"
RESTORE_HINT="nothing is committed or tagged; restore the tree with: git checkout -- $VERSION_FILE"
go build ./... || fail "the root module does not build; $RESTORE_HINT"
go test -count=1 -run '^(TestVersionFormat|TestDefaultClientUserAgent)$' ./durable ||
    fail "the User-Agent tests failed for Version $CONST_VERSION; $RESTORE_HINT"

# Step 2: pin the root module requirement in each nested module that has one.
for mod in $V0_MODULES $INTERNAL_MODULES; do
    case " $PINNED_MODULES " in
        *" $mod "*)
            echo "==> $mod: require $ROOT_MODULE $VERSION"
            go mod edit -require="$ROOT_MODULE@$VERSION" "$mod/go.mod"
            # The local replace still applies here, so this checks the nested
            # module against the root module at this commit.
            (cd "$mod" && go build ./...)
            ;;
        *)
            echo "==> $mod: does not depend on $ROOT_MODULE; nothing to pin"
            ;;
    esac
done

# Step 3: commit the Version constant and the pins.
if [ -n "$(git status --porcelain)" ]; then
    git add -A
    git commit -q -m "chore(release): $VERSION"
    echo "==> committed chore(release): $VERSION"
else
    echo "==> Version constant and pins already at $VERSION; nothing to commit"
fi

# Step 4: tag. A tag that exists already points at HEAD (checked above).
for tag in $TAGS; do
    case " $EXISTING_TAGS " in
        *" $tag "*)
            echo "==> $tag already tagged at HEAD"
            ;;
        *)
            git tag -a "$tag" -m "$tag"
            echo "==> tagged $tag"
            ;;
    esac
done

# On a detached HEAD (resuming on a checked-out release commit) there is no
# branch to push; the tags are enough.
BRANCH=$(git rev-parse --abbrev-ref HEAD)
[ "$BRANCH" != HEAD ] || BRANCH=
echo
echo "Release $VERSION prepared at $(git rev-parse --short HEAD)."
echo "Review the commit and tags, then push them with:"
echo
echo "  git push origin $BRANCH $TAGS"

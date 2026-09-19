#!/bin/sh
# Prepare a release of the root module and the published nested modules.
#
# Usage:
#   scripts/release.sh vX.Y.Z[-pre]
#
# All published modules share one version number per release. The script:
#
#   1. Pins every nested module that depends on the root module to the
#      version being released, so that an external consumer of a published
#      module resolves a root version that exists. The local `replace`
#      directive stays in place: consumers ignore it, and it keeps
#      in-repository development building against the checked-out root
#      module. Internal modules are pinned too, because a module that
#      depends on both the root module and a published nested module must
#      record the same root version as that nested module.
#   2. Commits that change as "chore(release): vX.Y.Z" (skipped when the
#      pins already match, so the script can be re-run on a merged commit).
#   3. Creates the tags the Go toolchain expects: vX.Y.Z for the root
#      module and <dir>/vX.Y.Z for each nested module. A tag that already
#      points at the release commit is kept, so the script can resume a
#      release that stopped after some tags were pushed. A tag that points
#      anywhere else is an error: the version is taken.
#
# Nothing is pushed. The script prints the push command to run afterwards.
# See CONTRIBUTING.md, "Releasing", for the full procedure.
#
# Requires a clean working tree.

set -eu

ROOT=$(cd "$(dirname "$0")/.." && pwd)

# Nested modules published for users, as directories relative to ROOT.
# These get a <dir>/vX.Y.Z tag.
PUBLISHED_MODULES="insight analysis"

# Nested modules used only inside the repository. They are pinned like the
# published ones but not tagged.
INTERNAL_MODULES="conformance examples"

usage() {
    echo "usage: $0 vX.Y.Z[-pre]" >&2
    exit 2
}

fail() {
    echo "release: $*" >&2
    exit 1
}

[ $# -eq 1 ] || usage
VERSION=$1

# Semantic version with a "v" prefix and an optional pre-release suffix.
if ! printf '%s\n' "$VERSION" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$'; then
    fail "version must look like vX.Y.Z or vX.Y.Z-pre, got '$VERSION'"
fi

# A v2+ release changes every module path (a /v2 suffix), which this script
# does not do.
MAJOR=$(printf '%s' "$VERSION" | sed -E 's/^v([0-9]+)\..*/\1/')
[ "$MAJOR" -lt 2 ] || fail "major version $MAJOR needs a module path suffix; this script handles v0 and v1 only"

cd "$ROOT"

[ -z "$(git status --porcelain)" ] || fail "working tree is not clean; commit or stash first"

ROOT_MODULE=$(sed -n 's/^module[[:space:]]*//p' go.mod)
[ -n "$ROOT_MODULE" ] || fail "cannot read module path from go.mod"

TAGS="$VERSION"
for mod in $PUBLISHED_MODULES; do
    [ -f "$mod/go.mod" ] || fail "published module '$mod' has no go.mod"
    TAGS="$TAGS $mod/$VERSION"
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

# Which nested modules depend on the root module, and do they already pin
# the release version? Decided before anything is edited, so that a refusal
# leaves the tree untouched.
PINNED_MODULES=
PINS_MATCH=yes
for mod in $PUBLISHED_MODULES $INTERNAL_MODULES; do
    [ -f "$mod/go.mod" ] || fail "nested module '$mod' has no go.mod"
    current=$(go mod edit -json "$mod/go.mod" | tr -d ' \t\n' \
        | sed -n "s|.*\"Path\":\"$ROOT_MODULE\",\"Version\":\"\([^\"]*\)\".*|\1|p")
    if [ -n "$current" ]; then
        PINNED_MODULES="$PINNED_MODULES $mod"
        [ "$current" = "$VERSION" ] || PINS_MATCH=no
    fi
done

# Resuming needs the pins in place: a new pin commit would move HEAD away
# from the tags that already exist.
if [ -n "$EXISTING_TAGS" ] && [ "$PINS_MATCH" = no ]; then
    fail "tag(s)$EXISTING_TAGS exist at HEAD, but the nested modules do not pin $VERSION"
fi

# Step 1: pin the root module requirement in each nested module that has one.
for mod in $PUBLISHED_MODULES $INTERNAL_MODULES; do
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

# Step 2: commit the pins.
if [ -n "$(git status --porcelain)" ]; then
    git add -A
    git commit -q -m "chore(release): $VERSION"
    echo "==> committed chore(release): $VERSION"
else
    echo "==> pins already at $VERSION; nothing to commit"
fi

# Step 3: tag. A tag that exists already points at HEAD (checked above).
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

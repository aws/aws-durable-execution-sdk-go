#!/bin/sh
# Check that the published nested modules can be consumed from outside the
# repository: a throwaway module in a temporary directory imports each
# nested module by its published path and must build.
#
# Usage:
#   scripts/verify-consumable.sh [--root-from-checkout | --released vX.Y.Z] [module...]
#
# Modes (how the throwaway module resolves the repository's modules):
#
#   default              The nested module comes from this checkout. The root
#                        module comes from the module proxy at the version the
#                        nested go.mod pins. This is the situation of a user
#                        who fetched the nested module at this commit, and it
#                        fails when the pinned root version is not published.
#   --root-from-checkout Both the nested and the root module come from this
#                        checkout. Nothing is fetched from the proxy for them.
#                        Suitable for every pull request; it checks the module
#                        graph a consumer sees (module paths, requirements)
#                        without needing a published root version.
#   --released vX.Y.Z    Nothing comes from the checkout. The throwaway module
#                        runs `go get <module>@vX.Y.Z` against the module proxy.
#                        Run after the tags are pushed.
#
# Modules default to the published nested modules: insight analysis.

set -eu

ROOT=$(cd "$(dirname "$0")/.." && pwd)

MODE=default
RELEASED=
while [ $# -gt 0 ]; do
    case "$1" in
        --root-from-checkout) MODE=checkout ;;
        --released)
            MODE=released
            shift
            [ $# -gt 0 ] || { echo "usage: --released needs a version" >&2; exit 2; }
            RELEASED=$1
            ;;
        --*) echo "unknown option: $1" >&2; exit 2 ;;
        *) break ;;
    esac
    shift
done

if [ $# -gt 0 ]; then
    MODULES=$*
else
    MODULES="insight analysis"
fi

ROOT_MODULE=$(sed -n 's/^module[[:space:]]*//p' "$ROOT/go.mod")

WORK=$(mktemp -d -t consumable.XXXXXX)
trap 'rm -rf "$WORK"' EXIT

# No workspace file and no parent go.mod may influence the throwaway module.
export GOWORK=off
export GOFLAGS=-mod=mod

status=0
for mod in $MODULES; do
    MOD_PATH=$(sed -n 's/^module[[:space:]]*//p' "$ROOT/$mod/go.mod")
    [ -n "$MOD_PATH" ] || { echo "cannot read module path from $mod/go.mod" >&2; exit 1; }

    dir="$WORK/$mod"
    mkdir -p "$dir"
    cd "$dir"
    go mod init example.com/consumer >/dev/null 2>&1

    # Import every non-main package of the module so that the build has to
    # resolve all of them. The package list comes from this checkout in every
    # mode; in --released mode the code itself still comes from the proxy.
    pkgs=$(cd "$ROOT/$mod" && go list -f '{{if ne .Name "main"}}{{.ImportPath}}{{end}}' ./...)
    [ -n "$pkgs" ] || { echo "no importable packages in $mod" >&2; exit 1; }
    {
        echo "package main"
        echo
        echo "import ("
        for p in $pkgs; do echo "	_ \"$p\""; done
        echo ")"
        echo
        echo "func main() {}"
    } > main.go

    case "$MODE" in
        default)
            go mod edit -replace="$MOD_PATH=$ROOT/$mod"
            go mod edit -require="$MOD_PATH@v0.0.0"
            ;;
        checkout)
            go mod edit -replace="$MOD_PATH=$ROOT/$mod"
            go mod edit -require="$MOD_PATH@v0.0.0"
            go mod edit -replace="$ROOT_MODULE=$ROOT"
            go mod edit -require="$ROOT_MODULE@v0.0.0"
            ;;
        released)
            go mod edit -require="$MOD_PATH@$RELEASED"
            ;;
    esac

    echo "==> $MOD_PATH ($MODE)"
    if go mod tidy && go build ./... ; then
        echo " -> ok"
    else
        echo " -> FAILED: $MOD_PATH is not consumable in mode '$MODE'" >&2
        status=1
    fi
done

exit $status

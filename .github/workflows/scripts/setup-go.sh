#!/usr/bin/env bash
# Source this file so the following preparation commands use the selected Go.
# The shared telemetry workflow installs no Go toolchain, and actions/setup-go
# cannot run inside a shell command.
durable_setup_go() {
  local version=1.25.14 min_minor=25 arch checksum install_root archive go_bin current
  case "$(uname -s)/$(uname -m)" in
    Linux/x86_64)
      arch=amd64
      checksum=a21ae5633a269bcd7e90cf767e48225633795e99d831742cbf3397064fee7712
      ;;
    Linux/aarch64|Linux/arm64)
      arch=arm64
      checksum=9bf234ea70ffec9347fdf6b22ce4add51717d3386a38a441e8c8743fceb5eaee
      ;;
    *) echo "Go CI setup requires Linux amd64 or arm64" >&2; return 1 ;;
  esac

  # Reuse an installed Go 1.<min_minor> or newer. Otherwise download the pinned release.
  current=
  if command -v go >/dev/null 2>&1; then
    current=$(go env GOVERSION 2>/dev/null | sed -n 's/^go1\.\([0-9][0-9]*\).*/\1/p')
  fi
  if [ -n "$current" ] && [ "$current" -ge "$min_minor" ]; then
    go_bin=$(dirname "$(command -v go)")
  else
    install_root=$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/durable-go.XXXXXX") || return 1
    archive="go${version}.linux-${arch}.tar.gz"
    curl --fail --silent --show-error --location --retry 3 \
      "https://go.dev/dl/${archive}" \
      --output "${install_root}/${archive}" || return 1
    # Values are the sha256 fields of go${version} in https://go.dev/dl/?mode=json&include=all.
    # Never unpack an unverified response, even when this file is sourced without errexit.
    printf '%s  %s\n' "$checksum" "${install_root}/${archive}" |
      sha256sum --check --status || {
        echo "Go archive checksum mismatch" >&2
        return 1
      }
    tar -xzf "${install_root}/${archive}" -C "$install_root" || return 1
    go_bin="${install_root}/go/bin"
  fi

  export PATH="${go_bin}:${PATH}"
  if [ -n "${GITHUB_PATH:-}" ]; then
    printf '%s\n' "$go_bin" >> "$GITHUB_PATH" || return 1
  fi
  go version
}

durable_setup_go

#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
toolchain=/workspace/.toolchains/go1.27.3
if [[ ! -x "$toolchain/go/bin/go" ]]; then
  archive=$(mktemp /tmp/game-go.XXXXXX.tar.gz)
  staging=$(mktemp -d /workspace/.toolchains-go.XXXXXX)
  trap 'rm -f "$archive"; rm -rf "$staging"' EXIT
  curl --fail --location --silent --show-error https://go.dev/dl/go1.27.3.linux-amd64.tar.gz -o "$archive"
  echo "65efbc80fdc1fa2cd2631cad5b481777a68e8725f34e89f50fc6ac714e379b40  $archive" | sha256sum --check --status
  tar -xzf "$archive" -C "$staging"
  mkdir -p /workspace/.toolchains
  mv "$staging" "$toolchain"
fi
source scripts/env.sh
go version
if ! go mod download; then
  # The managed proxy can block the module proxy's large-archive redirect.
  # Fetch the same pinned module from its allowed upstream, with sumdb enabled.
  GOPROXY=direct go mod download github.com/klauspost/compress@v1.16.7
  go mod download
fi
go mod verify
go build -o bin/game-server ./cmd/server
go build -o bin/actor-server ./cmd/actor-server
go build -o bin/loadgen ./cmd/loadgen

#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "$0")/../.." && pwd)
if (($# == 0)); then
  set -- up
fi

exec bash "$repo_root/infra/mongo/mongo.sh" "$@"

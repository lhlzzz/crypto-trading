#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
host="${BIAN_API_HOST:-0.0.0.0}"
port="${BIAN_API_PORT:-8001}"

if ss -ltn 2>/dev/null | awk -v needle=":${port}" '$4 ~ needle {found=1} END {exit found ? 0 : 1}'; then
  echo "bian API already listening on :${port}"
  exit 0
fi

cd "$script_dir"
exec python3 -m uvicorn bian_api:app --host "$host" --port "$port"

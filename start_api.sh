#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
host="${BIAN_API_HOST:-0.0.0.0}"
port="${BIAN_API_PORT:-8001}"
if [ -n "${BIAN_PYTHON_BIN:-}" ]; then
  python_bin="$BIAN_PYTHON_BIN"
elif [ -x "$script_dir/.venv/bin/python" ]; then
  python_bin="$script_dir/.venv/bin/python"
else
  python_bin="python3.12"
fi

if ! command -v "$python_bin" >/dev/null 2>&1; then
  echo "bian Python runtime not found: $python_bin" >&2
  exit 1
fi

bian_api_healthy() {
  "$python_bin" - "$port" <<'PY'
import json
import sys
from urllib.request import urlopen

port = int(sys.argv[1])
try:
    with urlopen(f"http://127.0.0.1:{port}/health", timeout=2) as response:
        payload = json.load(response)
except Exception:
    raise SystemExit(1)
raise SystemExit(0 if payload.get("service") == "bian" else 1)
PY
}

if ss -ltn 2>/dev/null | awk -v needle=":${port}" '$4 ~ needle {found=1} END {exit found ? 0 : 1}'; then
  if bian_api_healthy; then
    echo "bian API already listening on :${port}"
    exit 0
  fi
  echo "port :${port} is occupied by a non-bian service" >&2
  exit 1
fi

cd "$script_dir"
exec "$python_bin" -m uvicorn bian_api:app --host "$host" --port "$port"

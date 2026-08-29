#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python_bin="${BIAN_PYTHON_BIN:-$script_dir/.venv/bin/python}"

if [ ! -x "$python_bin" ]; then
  echo "bian Python runtime not found: $python_bin" >&2
  exit 1
fi

export BIAN_MARKET=FUTURES
export BIAN_MODE=paper
export LIVE_TRADING_ENABLED=false
cd "$script_dir"
exec "$python_bin" paper_runner.py "$@"

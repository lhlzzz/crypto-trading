#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python_bin="${BIAN_PYTHON_BIN:-$script_dir/.venv/bin/python}"

if [ ! -x "$python_bin" ]; then
  echo "bian Python runtime not found: $python_bin" >&2
  exit 1
fi
if [ "${BIAN_MODE:-testnet}" != "testnet" ]; then
  echo "start_testnet.sh requires BIAN_MODE=testnet" >&2
  exit 1
fi
if [ -z "${BIAN_TESTNET_API_KEY:-}" ] || [ -z "${BIAN_TESTNET_API_SECRET:-}" ]; then
  echo "BIAN_TESTNET_API_KEY and BIAN_TESTNET_API_SECRET are required for Testnet" >&2
  exit 1
fi

export BIAN_MODE=testnet
cd "$script_dir"
exec "$python_bin" paper_runner.py --mode testnet "$@"

#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python_bin="${BIAN_PYTHON_BIN:-$script_dir/.venv/bin/python}"

if [ ! -x "$python_bin" ]; then
  echo "bian Python runtime not found: $python_bin" >&2
  exit 1
fi
if [ -z "${BIAN_TESTNET_API_KEY:-}" ] || [ -z "${BIAN_TESTNET_API_SECRET:-}" ]; then
  echo "BIAN_TESTNET_API_KEY and BIAN_TESTNET_API_SECRET are required for Testnet" >&2
  exit 1
fi

export BIAN_MARKET=FUTURES
export BIAN_MODE=testnet
cd "$script_dir"
if ! "$python_bin" - <<'PY'
from binance_client import ClientConfig, FuturesPrivateClient, FuturesPublicClient
from runtime_gate import evaluate_runtime_gate

config = ClientConfig.from_env("testnet")
public = FuturesPublicClient(config)
info = public.get_exchange_info()
if not info:
    raise SystemExit("TESTNET HARD BLOCK: exchangeInfo unavailable")
client = FuturesPrivateClient(config)
snapshot = client.account_snapshot()
if snapshot.position_mode != "ONE_WAY":
    raise SystemExit("TESTNET HARD BLOCK: position mode must be ONE_WAY")
listen_key = client.create_listen_key()
if not listen_key:
    raise SystemExit("TESTNET HARD BLOCK: user stream listenKey failed")
gate = evaluate_runtime_gate(mode="testnet", client=client, probe_account=True)
print(gate.as_dict())
if not gate.credentials_ok or not gate.account_reachable:
    raise SystemExit("TESTNET HARD BLOCK: account preflight failed")
PY
then
  echo "TESTNET HARD BLOCK: preflight failed" >&2
  exit 1
fi
exec "$python_bin" paper_runner.py --mode testnet "$@"

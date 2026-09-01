#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python_bin="${BIAN_PYTHON_BIN:-$script_dir/.venv/bin/python}"

if [ "${BIAN_MODE:-}" != "live" ]; then
  echo "LIVE HARD BLOCK: BIAN_MODE must equal live" >&2
  exit 1
fi
if [ "${BIAN_MARKET:-}" != "FUTURES" ]; then
  echo "LIVE HARD BLOCK: BIAN_MARKET must equal FUTURES" >&2
  exit 1
fi
if [ "${POSITIONING_DECISION_ENABLED:-false}" != "true" ]; then
  echo "LIVE HARD BLOCK: POSITIONING_DECISION_ENABLED must equal true" >&2
  exit 1
fi
if [ "${LIVE_TRADING_ENABLED:-false}" != "true" ]; then
  echo "LIVE HARD BLOCK: LIVE_TRADING_ENABLED must equal true" >&2
  exit 1
fi
if [ -z "${LIVE_CONFIRMATION_TOKEN:-}" ]; then
  echo "LIVE HARD BLOCK: LIVE_CONFIRMATION_TOKEN is required" >&2
  exit 1
fi
if [ ! -x "$python_bin" ]; then
  echo "bian Python runtime not found: $python_bin" >&2
  exit 1
fi
if [ -z "${BIAN_LIVE_API_KEY:-}" ] || [ -z "${BIAN_LIVE_API_SECRET:-}" ]; then
  echo "BIAN_LIVE_API_KEY and BIAN_LIVE_API_SECRET are required for Live" >&2
  exit 1
fi
if [ "${FUTURES_POSITION_MODE:-ONE_WAY}" != "ONE_WAY" ]; then
  echo "LIVE HARD BLOCK: FUTURES_POSITION_MODE must equal ONE_WAY" >&2
  exit 1
fi
if [ "${FUTURES_MARGIN_MODE:-ISOLATED}" != "ISOLATED" ]; then
  echo "LIVE HARD BLOCK: FUTURES_MARGIN_MODE must equal ISOLATED" >&2
  exit 1
fi

cat <<BANNER
================================
BIAN LIVE TRADING
MODE: LIVE
LIVE_TRADING_ENABLED: TRUE
ACCOUNT: Binance USD-M Futures
RISK LIMITS: environment configured
MAX ORDER: ${MAX_ORDER_USDT:-100}
MAX POSITION: ${MAX_POSITION_USDT:-500}
MAX DAILY LOSS: ${MAX_DAILY_LOSS_USDT:-50}
================================
BANNER
read -r -p "Type LIVE_CONFIRMATION_TOKEN to continue: " confirmation
if [ "$confirmation" != "$LIVE_CONFIRMATION_TOKEN" ]; then
  echo "LIVE HARD BLOCK: confirmation did not match" >&2
  exit 1
fi

export BIAN_LIVE_CONFIRMATION="$confirmation"
cd "$script_dir"
if ! "$python_bin" - <<'PY'
from runtime_gate import evaluate_runtime_gate
result = evaluate_runtime_gate(mode="live", probe_account=True)
print(result.as_dict())
raise SystemExit(0 if result.live_allowed else 1)
PY
then
  echo "LIVE HARD BLOCK: runtime preflight failed" >&2
  exit 1
fi
exec "$python_bin" paper_runner.py --mode live "$@"

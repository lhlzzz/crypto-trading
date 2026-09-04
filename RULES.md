# Rules

- Public market collection remains owned by `scripts/bian_market.py`.
- Persist capture time and source URL.
- `database.py` owns schema and connections only; it contains no trading logic.
- `binance_client.py` is the only Binance adapter and keeps public/private
  clients permanently separate.
- `bian_api.py` remains read-only and is never an execution gateway.
- `engine.py` may only produce TradeIntent objects.
- `execution.py` is the only order-submission owner.
- Paper, testnet, and live modes must share one execution interface.
- Live orders require both `BIAN_MODE=live` and
  `LIVE_TRADING_ENABLED=true`; otherwise private live access is hard-blocked.
- Live startup additionally requires `LIVE_CONFIRMATION_TOKEN` and explicit
  operator confirmation; paper and testnet can never target live endpoints.
- API credentials come only from environment or secret configuration and must
  never be logged, committed, or exposed through the dashboard.
- No Polymarket references, duplicate engines, duplicate execution systems,
  or copied third-party trading frameworks.
- User Data Stream is isolated from public market streams and cannot override
  REST reconciliation as final truth.
- `runtime_gate.py` owns the canonical readiness result. Its `live_allowed`
  field is false until the externally verified release gates pass.
- Futures public observation is primary; Spot is confirmation-only and has no
  private trading or account authority.
- Unknown, stale, future, or timestamp-inconsistent evidence is fail-closed
  and cannot create a strategy CLOSE, REDUCE, or OPEN. Emergency flatten is
  not a strategy CLOSE.
- Canonical USD-M universe is BTCUSDT, ETHUSDT, and BNBUSDT only.
- Store trading-state methods require explicit `mode=`; no `BIAN_MODE` fallback.
- Live CREATE, CANCEL, and CANCEL_ALL require `authorize_live_order_mutation`.
- Runtime gate is re-evaluated every cycle; a startup snapshot is not reused.

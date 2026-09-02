# Next Action

30M realtime PASSED. Do not rerun 30M unless freeze-path code changes.

Next elapsed gate is realtime 2H:

```bash
python scripts/validate_runtime.py --stage realtime_2h --duration 7200
```

Required healthy channels: TRADE, BOOK_TICKER, DEPTH, MARK_INDEX_FUNDING,
LIQUIDATION_LIVENESS. Dedicated markPrice is NOT_REQUIRED while production
liveness comes from MARK_INDEX_FUNDING.

REAL_DATA_READY stays false until realtime 24H PASSED. Paper 24H cannot start
before that. Testnet is BLOCKED until BIAN_TESTNET_API_KEY and
BIAN_TESTNET_API_SECRET are present. Live remains hard-blocked.

`POSITIONING_DECISION_ENABLED=false`.

Positioning decisions now persist episode identity and complete normalized
evidence metadata. Testnet/Live runtime gate and reconciliation consume the
canonical `FuturesAccountSnapshot`; user-stream reconnects are unavailable
until recovery is complete.

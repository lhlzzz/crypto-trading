# Next Action

Freeze-path and runtime-safety files changed. Prior realtime 30M/2H PASS
is EXPIRED and must not be reused.

Next elapsed gate after a fresh observer session:

```bash
python scripts/validate_runtime.py --stage realtime_30m --duration 1800
```

Required healthy channels: TRADE, BOOK_TICKER, DEPTH, MARK_INDEX_FUNDING,
LIQUIDATION_LIVENESS. Dedicated markPrice is NOT_REQUIRED while production
liveness comes from MARK_INDEX_FUNDING.

REAL_DATA_READY stays false until realtime 24H PASSED. Paper 24H cannot start
before that. Testnet is BLOCKED until BIAN_TESTNET_API_KEY and
BIAN_TESTNET_API_SECRET are present. Live remains hard-blocked.

`POSITIONING_DECISION_ENABLED=false`. `LIVE_ALLOWED=false`.

Universe is BTCUSDT / ETHUSDT / BNBUSDT only. Mixed unauthorized env fails
closed. User-stream reconnects remain unavailable until recovery is complete.

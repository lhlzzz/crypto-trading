# Next Action

Keep `POSITIONING_DECISION_ENABLED=false`. Do not add Futures private order
methods. Continue from `d75a795`; the suite is 120 tests and green.

Phase A and Phase B are complete. Next is Phase C / T36: USD-M Futures
orderbook as production liquidity evidence.

Phase C scope:

1. REST snapshot plus diff using `U`/`u`/`pu`.
2. Invalidate and resnapshot on gap.
3. Keep Spot book as confirmation only.

Restart `scripts/bian_market.py observe` after the Futures book lands so
freshness can recover. T33 elapsed 1h/6h/24h/7-day gates remain open and
are not skipped. Do not add Futures private order methods.

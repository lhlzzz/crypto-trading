# Next Action

Actual blockers are external runtime evidence, not implementation debt:

- continuous public observation: 1h, 6h, and 24h gates
- seven-day shadow with directional samples
- Testnet USD-M lifecycle, blocked until credentials are supplied
- Live release gates: observation, shadow, Testnet lifecycle, reconciliation,
  data health, Meme `TRADEABLE`, and human confirmation

Code verification completed on 2026-08-31: 246 tests pass, compileall passes,
diff check passes, and PostgreSQL schema/status is healthy. A direct public
smoke reached `markPrice`, but OI, funding, aggregate trades, ticker
collection, and Futures WebSocket observation failed with SSL EOF or
connection-reset errors. External runtime evidence remains uncompleted.

Not verified: 30m, 2h, 6h, and 24h realtime; 24h Paper; 7d Shadow; Alpha OOS;
and Testnet lifecycle.

`POSITIONING_DECISION_ENABLED=false` and Live remains hard-blocked.

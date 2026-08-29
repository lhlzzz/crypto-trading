# Next Action

Keep `POSITIONING_DECISION_ENABLED=false` until elapsed observation, 7-day
shadow, and Testnet USD-M lifecycle gates pass.

The trading contract is now futures-native: TradeIntent uses LONG/SHORT plus
OPEN/REDUCE/CLOSE, Risk/Paper/User Stream/Reconciliation no longer use Spot
private semantics, and `quote_quantity` is not an active trading field.

Do not enable Live. Live remains HARD BLOCKED until data health,
reconciliation, ONE_WAY/ISOLATED, meme TRADEABLE, and human confirmation
all pass. T33 elapsed 1h/6h/24h/7-day gates remain open.

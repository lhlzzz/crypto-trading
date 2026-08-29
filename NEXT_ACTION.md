# Next Action

Keep `POSITIONING_DECISION_ENABLED=false`. Continue from `e4dbafe`.

T49 audit and T50 adapter are the current Futures-native cutover. Next is
TradeIntent: `position_direction` LONG/SHORT, `position_action`
OPEN/REDUCE/CLOSE, `reduce_only`, explicit leverage, isolated, one-way.

Do not rewrite Paper/Risk until that contract exists. T36 Futures orderbook
stays deferred. T33 elapsed 1h/6h/24h/7-day gates remain open and are not
skipped.

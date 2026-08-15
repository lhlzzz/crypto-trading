# State

The `bian` bot owns Binance ticker and product-coverage snapshots in the
`bian` PostgreSQL database on port `5446`. Its read-only FastAPI contract runs
on port `8001` and is consumed by Financial OS; `bian` remains the sole
database owner.

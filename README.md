# bian

Independent Binance public market-data bot.

Repository name: `crypto trading`

The production boundary is `PUBLIC_READ_ONLY / NO_TRADE`: Binance public
endpoints are collected by `scripts/bian_market.py`, PostgreSQL is the only
bian data authority, and Financial OS owns the browser UI.

```bash
python3 -m pip install -r requirements.txt
python3 scripts/database.py ensure
python3 scripts/bian_market.py collect
python3 scripts/database.py status
bash start_api.sh
```

The read-only API exposes `/health` and
`/api/os/front-data`. Financial OS proxies that contract through
`BIAN_API_BASE_URL` and displays it at
`http://localhost:3000/dashboard/bian`.

The API contract and release identity default to `2026-08-15` and can be
overridden independently with `BIAN_OPERATOR_CONTRACT_VERSION` and
`BIAN_RELEASE_VERSION`.

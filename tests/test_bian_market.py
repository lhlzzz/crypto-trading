from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import bian_market  # noqa: E402
from database import configured_dsn  # noqa: E402


class BianMarketTests(unittest.TestCase):
    def test_uses_its_own_dsn(self):
        self.assertEqual(
            configured_dsn({"BIAN_PG_DSN": "postgresql://bian-test"}),
            "postgresql://bian-test",
        )

    def test_collects_binance_tickers_and_coverage(self):
        def fake_get(url: str, timeout_sec: float = 8.0):
            del timeout_sec
            if url.endswith("/ticker/24hr"):
                return [{"symbol": "BTCUSDT", "lastPrice": "100", "priceChangePercent": "2", "quoteVolume": "900"}]
            if url.endswith("/exchangeInfo"):
                return {"symbols": [{"symbol": "BTCUSDT"}], "optionSymbols": [{"symbol": "BTC-1-C"}]}
            raise AssertionError(url)

        with patch.object(bian_market, "_get_json", side_effect=fake_get):
            report = bian_market.collect(limit=1)

        self.assertEqual(report["markets"][0]["symbol"], "BTCUSDT")
        self.assertEqual(len(report["product_coverage"]), 4)


if __name__ == "__main__":
    unittest.main()

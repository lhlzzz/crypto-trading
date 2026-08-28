from __future__ import annotations

import asyncio
import socket
import subprocess
import sys
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

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
            report = bian_market.collect(limit=1, run_id="test-run")

        self.assertEqual(report["markets"][0]["symbol"], "BTCUSDT")
        self.assertEqual(report["markets"][0]["last_price"], Decimal("100"))
        self.assertEqual(report["run_id"], "test-run")
        self.assertEqual(len(report["product_coverage"]), 4)
        self.assertEqual(report["events"][0]["event_type"], "UNIVERSE_BREADTH")

    def test_collect_records_ticker_receipt_after_response(self):
        received_at = datetime(2026, 8, 26, 12, 0, 2, tzinfo=timezone.utc)
        close_time = datetime(2026, 8, 26, 12, 0, 1, tzinfo=timezone.utc)
        calls: list[str] = []

        def ticker_rows():
            calls.append("ticker")
            return ([{
                "symbol": "BTCUSDT", "lastPrice": "100",
                "priceChangePercent": "2", "quoteVolume": "900",
                "closeTime": int(close_time.timestamp() * 1000),
            }], "https://api.binance.com/api/v3/ticker/24hr")

        def now() -> str:
            calls.append("now")
            return received_at.isoformat()

        with patch.object(bian_market, "_universe_ticker_rows", side_effect=ticker_rows), patch.object(
            bian_market, "_now", side_effect=now
        ), patch.object(bian_market, "_coverage", return_value=[]):
            report = bian_market.collect(limit=1, run_id="receipt-order")

        self.assertEqual(calls[:2], ["ticker", "now"])
        self.assertEqual(report["markets"][0]["latency_ms"], 1000)

    def test_universe_scanner_derives_breadth_candidates_and_regime(self):
        rows = [
            {
                "symbol": "BTCUSDT", "lastPrice": "102", "highPrice": "102",
                "lowPrice": "98", "priceChangePercent": "2", "quoteVolume": "1000",
            },
            {
                "symbol": "ETHUSDT", "lastPrice": "51", "highPrice": "51",
                "lowPrice": "49", "priceChangePercent": "2", "quoteVolume": "900",
            },
            {
                "symbol": "SOLUSDT", "lastPrice": "9", "highPrice": "12",
                "lowPrice": "9", "priceChangePercent": "-3", "quoteVolume": "100",
            },
        ]

        features = bian_market.universe_features(rows, candidate_limit=2)

        self.assertEqual(features["advancers"], 2)
        self.assertEqual(features["decliners"], 1)
        self.assertEqual(features["new_highs"], 2)
        self.assertEqual(features["new_lows"], 1)
        self.assertEqual(features["market_regime"], "RISK_ON")
        self.assertEqual(features["candidate_symbols"], ["BTCUSDT", "ETHUSDT"])

    def test_candidate_stream_symbols_keep_benchmarks_and_deduplicate_tier_two(self):
        candidates = bian_market._candidate_stream_symbols(
            {"universe": {"candidate_symbols": ["SOLUSDT", "BTCUSDT", "SOLUSDT"]}},
            fallback_symbols=["BTC-USDT", "ETH-USDT", "ADA-USDT"],
        )

        self.assertEqual(
            candidates,
            ["BTC-USDT", "ETH-USDT", "SOL-USDT", "ADA-USDT"],
        )

    def test_positioning_observation_cycle_persists_spot_and_futures_without_private_clients(self):
        spot_report = {
            "run_id": "spot-run",
            "universe": {"candidate_symbols": ["SOLUSDT"]},
        }
        futures_report = {"run_id": "futures-run", "events": []}
        with patch.object(bian_market, "collect", return_value=spot_report) as collect, patch.object(
            bian_market, "collect_futures_observations", return_value=futures_report
        ) as futures, patch.object(bian_market, "persist") as persist:
            candidates = bian_market.collect_positioning_observations(
                fallback_symbols=["BTC-USDT", "ETH-USDT"],
                candidate_limit=3,
                dsn="postgresql://bian-test",
            )

        self.assertEqual(candidates, ["BTC-USDT", "ETH-USDT", "SOL-USDT"])
        collect.assert_called_once()
        futures.assert_called_once()
        self.assertEqual(
            futures.call_args.args[0], ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        )
        self.assertEqual(persist.call_count, 2)

    def test_retries_transient_http_failure(self):
        response = MagicMock()
        response.read.return_value = b'{"ok": true}'
        response.__enter__.return_value = response
        transient = bian_market.urllib.error.HTTPError(
            "https://example.test",
            503,
            "Service Unavailable",
            {},
            None,
        )
        with patch.object(
            bian_market.urllib.request,
            "urlopen",
            side_effect=[transient, response],
        ), patch.object(bian_market.time, "sleep") as sleep:
            payload = bian_market._get_json(
                "https://example.test",
                attempts=2,
            )

        self.assertEqual(payload, {"ok": True})
        sleep.assert_called_once()

    def test_uses_explicit_or_standard_proxy_for_rest_snapshots(self):
        with patch.dict(
            __import__("os").environ,
            {"BIAN_HTTP_PROXY": "http://explicit-proxy:7897"},
            clear=True,
        ):
            self.assertEqual(bian_market._http_proxy(), "http://explicit-proxy:7897")
        with patch.dict(
            __import__("os").environ,
            {"HTTPS_PROXY": "http://standard-proxy:7897"},
            clear=True,
        ):
            self.assertEqual(bian_market._http_proxy(), "http://standard-proxy:7897")

    def test_persist_uses_run_and_symbol_conflict_target(self):
        cursor = MagicMock()
        connection = MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.__enter__.return_value = cursor
        report = {
            "run_id": "79b6f086-7417-4a83-ad83-4d33d2cb34f1",
            "collection_kind": "rest_24h",
            "captured_at": "2026-08-15T00:00:00+00:00",
            "source_url": "https://api.binance.com/api/v3/ticker/24hr",
            "markets": [
                {
                    "symbol": "BTCUSDT",
                    "last_price": Decimal("100"),
                    "price_change_percent": Decimal("2"),
                    "quote_volume": Decimal("900"),
                    "payload": {"symbol": "BTCUSDT"},
                }
            ],
            "product_coverage": [],
        }
        with patch.object(bian_market, "ensure_schema"), patch(
            "psycopg2.connect",
            return_value=connection,
        ):
            bian_market.persist(report)

        statements = "\n".join(
            call.args[0]
            for call in cursor.execute.call_args_list
        )
        self.assertIn(
            "ON CONFLICT (run_id, symbol) WHERE run_id IS NOT NULL",
            statements,
        )

    def test_persist_writes_force_orders_to_the_liquidation_audit_table(self):
        cursor = MagicMock()
        connection = MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.__enter__.return_value = cursor
        report = {
            "run_id": "79b6f086-7417-4a83-ad83-4d33d2cb34f2",
            "collection_kind": "stream_futures_liquidation",
            "captured_at": "2026-08-26T00:00:00+00:00",
            "source_url": "wss://fstream.binance.com",
            "markets": [],
            "events": [{
                "event_id": "79b6f086-7417-4a83-ad83-4d33d2cb34f3",
                "symbol": "BTCUSDT", "market": "FUTURES",
                "event_type": "FORCE_ORDER",
                "source_timestamp": "2026-08-26T00:00:00+00:00",
                "received_timestamp": "2026-08-26T00:00:01+00:00",
                "latency_ms": 1000, "price": "100", "quantity": "2",
                "direction": "SELL",
            }],
            "product_coverage": [],
        }
        with patch.object(bian_market, "ensure_schema"), patch(
            "psycopg2.connect", return_value=connection
        ):
            bian_market.persist(report)

        statements = "\n".join(call.args[0] for call in cursor.execute.call_args_list)
        self.assertIn("INSERT INTO liquidation_events", statements)

    def test_builds_public_only_binance_trade_feed(self):
        from cryptofeed.symbols import Symbols

        async def callback(ticker, receipt_timestamp):
            del ticker, receipt_timestamp

        with patch.object(Symbols, "populated", return_value=True), patch.object(
            Symbols,
            "get",
            return_value=({"BTC-USDT": "BTCUSDT"}, None),
        ):
            handler = bian_market._build_stream_handler(["BTC-USDT"], callback)
        feed = handler.feeds[0]
        self.assertEqual(feed.id, "BINANCE")
        self.assertFalse(feed.requires_authentication)
        self.assertEqual(feed._feed_config, {"trades": ["BTC-USDT"]})

    def test_rejects_non_usdt_stream_symbols(self):
        with self.assertRaisesRegex(ValueError, "USDT"):
            bian_market._stream_symbols("BTC-USDC")

    def test_stream_trade_uses_the_actual_trade_price(self):
        trade = SimpleNamespace(
            exchange="BINANCE",
            symbol="BTC-USDT",
            price=Decimal("100.123456789"),
            amount=Decimal("0.25"),
            side="buy",
            timestamp=1_786_493_000.0,
        )

        symbol, snapshot = bian_market._stream_market(trade, 1_786_493_001.0)

        self.assertEqual(symbol, "BTC-USDT")
        self.assertEqual(snapshot["symbol"], "BTCUSDT")
        self.assertEqual(snapshot["last_price"], Decimal("100.123456789"))
        self.assertEqual(snapshot["payload"]["amount"], "0.25")

    def test_trade_flow_excludes_future_events_and_uses_taker_direction(self):
        captured = bian_market._event_datetime("2026-08-26T00:00:02+00:00")
        aggregate = bian_market.aggregate_trade_flow(
            [
                {"event_timestamp": "2026-08-26T00:00:01+00:00", "quantity": "3", "buyer_maker": False},
                {"event_timestamp": "2026-08-26T00:00:03+00:00", "quantity": "10", "buyer_maker": True},
            ],
            as_of=captured,
        )
        self.assertEqual(aggregate.buy_volume, Decimal("3"))
        self.assertEqual(aggregate.sell_volume, Decimal("0"))
        self.assertEqual(aggregate.cvd, Decimal("3"))

    def test_market_data_envelope_normalizer_recomputes_latency(self):
        event = bian_market._normalize_market_data_event(
            {
                "event_id": "trade", "symbol": "btcusdt", "market": "SPOT",
                "event_type": "TRADE",
                "source_timestamp": "2026-08-26T12:00:00+00:00",
                "received_timestamp": "2026-08-26T12:00:01+00:00",
                "latency_ms": 0,
            },
            default_source="binance-test",
        )

        self.assertEqual(event["symbol"], "BTCUSDT")
        self.assertEqual(event["source"], "binance-test")
        self.assertEqual(event["latency_ms"], 1000)

    def test_market_data_envelope_normalizer_rejects_out_of_order_clocks(self):
        with self.assertRaisesRegex(ValueError, "received_timestamp"):
            bian_market._normalize_market_data_event(
                {
                    "event_id": "trade", "symbol": "BTCUSDT", "market": "SPOT",
                    "event_type": "TRADE",
                    "source_timestamp": "2026-08-26T12:00:01+00:00",
                    "received_timestamp": "2026-08-26T12:00:00+00:00",
                },
                default_source="binance-test",
            )

    def test_persistence_error_code_omits_exception_text(self):
        self.assertEqual(
            bian_market._persistence_error_code(ValueError("database password=secret")),
            "persist_valueerror",
        )
        self.assertEqual(
            bian_market._collection_error_code(RuntimeError("token=secret")),
            "collection_runtimeerror",
        )

    def test_script_imports_root_level_market_data_contract(self):
        code = """
import sys
from pathlib import Path
root = Path.cwd()
sys.path[:] = [str(root / 'scripts'), *sys.path[1:]]
import bian_market
assert bian_market._market_data_envelope_type().__name__ == 'MarketDataEnvelope'
"""
        result = subprocess.run(
            [sys.executable, "-c", code],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, result.stderr)

    def test_positioning_features_deduplicate_duplicate_trade_event_ids(self):
        as_of = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)
        event = {
            "event_id": "same-trade", "event_type": "TRADE",
            "event_timestamp": "2026-08-26T11:59:30+00:00",
            "received_timestamp": "2026-08-26T11:59:30+00:00",
            "price": "100", "quantity": "3", "notional": "300",
            "metadata": {"buyer_maker": False},
        }

        features = bian_market.positioning_feature_values(
            [event, dict(event)], as_of=as_of
        )

        self.assertEqual(features["spot_buy_volume"], Decimal("3"))
        self.assertEqual(features["cvd_1m"], Decimal("3"))

    def test_positioning_features_use_persisted_metadata_and_received_boundary(self):
        as_of = "2026-08-26T00:05:00+00:00"
        events = [
            {
                "event_id": "trade-buy", "event_type": "TRADE",
                "event_timestamp": "2026-08-26T00:04:40+00:00",
                "received_timestamp": "2026-08-26T00:04:41+00:00",
                "quantity": "3", "metadata": {"buyer_maker": False},
            },
            {
                "event_id": "trade-sell", "event_type": "TRADE",
                "event_timestamp": "2026-08-26T00:04:45+00:00",
                "received_timestamp": "2026-08-26T00:04:46+00:00",
                "quantity": "2", "metadata": {"buyer_maker": True},
            },
            {
                "event_id": "delayed-trade", "event_type": "TRADE",
                "event_timestamp": "2026-08-26T00:04:50+00:00",
                "received_timestamp": "2026-08-26T00:05:01+00:00",
                "quantity": "50", "metadata": {"buyer_maker": False},
            },
            {
                "event_id": "oi-before", "event_type": "OPEN_INTEREST",
                "event_timestamp": "2026-08-26T00:04:00+00:00",
                "received_timestamp": "2026-08-26T00:04:01+00:00",
                "quantity": "100", "metadata": {"metadata": {"openInterest": "100"}},
            },
            {
                "event_id": "oi-latest", "event_type": "OPEN_INTEREST",
                "event_timestamp": "2026-08-26T00:04:50+00:00",
                "received_timestamp": "2026-08-26T00:04:51+00:00",
                "quantity": "110", "metadata": {"metadata": {"openInterest": "110"}},
            },
            {
                "event_id": "funding", "event_type": "FUNDING",
                "event_timestamp": "2026-08-26T00:04:55+00:00",
                "received_timestamp": "2026-08-26T00:04:56+00:00",
                "metadata": {"metadata": {"fundingRate": "0.0001"}},
            },
            {
                "event_id": "taker", "event_type": "TAKER_FLOW",
                "event_timestamp": "2026-08-26T00:04:55+00:00",
                "received_timestamp": "2026-08-26T00:04:56+00:00",
                "metadata": {"metadata": {"buyVol": "9", "sellVol": "4"}},
            },
            {
                "event_id": "mark", "event_type": "MARK_INDEX_FUNDING",
                "event_timestamp": "2026-08-26T00:04:55+00:00",
                "received_timestamp": "2026-08-26T00:04:56+00:00",
                "metadata": {"metadata": {"basisBps": "2.5", "lastFundingRate": "0.0002"}},
            },
            {
                "event_id": "long-liquidation", "event_type": "FORCE_ORDER",
                "event_timestamp": "2026-08-26T00:04:56+00:00",
                "received_timestamp": "2026-08-26T00:04:57+00:00",
                "notional": "250", "direction": "SELL", "metadata": {},
            },
        ]

        features = bian_market.positioning_feature_values(
            events,
            as_of=bian_market._event_datetime(as_of),
        )

        self.assertEqual(features["spot_buy_volume"], Decimal("3"))
        self.assertEqual(features["spot_sell_volume"], Decimal("2"))
        self.assertEqual(features["cvd_change"], Decimal("1"))
        self.assertEqual(features["oi_change"], Decimal("0.1"))
        self.assertEqual(features["funding_rate"], Decimal("0.0002"))
        self.assertEqual(features["taker_buy_volume"], Decimal("9"))
        self.assertEqual(features["basis_bps"], Decimal("2.5"))
        self.assertEqual(features["long_liquidation_notional"], Decimal("250"))
        self.assertEqual(features["short_liquidation_notional"], Decimal("0"))
        self.assertIn("spot_trade", features["source_timestamps"])

    def test_positioning_features_ignore_historical_liquidation_freshness(self):
        as_of = datetime(2026, 8, 26, 12, 10, tzinfo=timezone.utc)
        historical = as_of - timedelta(minutes=10)
        trade = as_of - timedelta(seconds=30)
        events = [
            {
                "event_id": "trade-current", "event_type": "TRADE",
                "event_timestamp": trade.isoformat(),
                "received_timestamp": trade.isoformat(),
                "price": "100", "quantity": "1", "notional": "100",
                "metadata": {"buyer_maker": False},
            },
            {
                "event_id": "force-historical", "event_type": "FORCE_ORDER",
                "event_timestamp": historical.isoformat(),
                "received_timestamp": historical.isoformat(),
                "notional": "250", "direction": "SELL", "metadata": {},
            },
        ]

        features = bian_market.positioning_feature_values(
            events,
            as_of=bian_market._event_datetime(as_of),
        )

        self.assertIsNone(features["long_liquidation_notional"])
        self.assertNotIn("futures_liquidation", features["source_timestamps"])

    def test_positioning_features_derive_multi_window_flow_volume_impact_and_oi(self):
        as_of = datetime(2026, 8, 26, 12, 10, tzinfo=timezone.utc)

        def trade(
            minute: int,
            price: str,
            quantity: str,
            *,
            buyer_maker: bool = False,
            second: int = 30,
        ):
            event_at = datetime(2026, 8, 26, 12, minute, second, tzinfo=timezone.utc)
            return {
                "event_id": f"trade-{minute}-{price}", "event_type": "TRADE",
                "event_timestamp": event_at.isoformat(),
                "received_timestamp": event_at.isoformat(),
                "price": price, "quantity": quantity,
                "notional": str(Decimal(price) * Decimal(quantity)),
                "metadata": {"buyer_maker": buyer_maker},
            }

        events = [
            trade(4, "100", "1"), trade(5, "110", "2"),
            trade(6, "100", "3"), trade(7, "100", "4"),
            trade(8, "100", "5"), trade(9, "110", "5"),
            trade(9, "109", "5", second=50),
            {
                "event_id": "oi-0", "event_type": "OPEN_INTEREST",
                "event_timestamp": "2026-08-26T12:00:00+00:00",
                "received_timestamp": "2026-08-26T12:00:01+00:00",
                "quantity": "100",
            },
            {
                "event_id": "oi-5", "event_type": "OPEN_INTEREST",
                "event_timestamp": "2026-08-26T12:05:00+00:00",
                "received_timestamp": "2026-08-26T12:05:01+00:00",
                "quantity": "110",
            },
            {
                "event_id": "oi-10", "event_type": "OPEN_INTEREST",
                "event_timestamp": "2026-08-26T12:10:00+00:00",
                "received_timestamp": "2026-08-26T12:10:00+00:00",
                "quantity": "121",
            },
            {
                "event_id": "book-1", "event_type": "ORDERBOOK",
                "event_timestamp": "2026-08-26T12:09:00+00:00",
                "received_timestamp": "2026-08-26T12:09:00+00:00",
                "metadata": {"liquidity_added": "10", "liquidity_removed": "3"},
            },
            {
                "event_id": "book-2", "event_type": "ORDERBOOK",
                "event_timestamp": "2026-08-26T12:10:00+00:00",
                "received_timestamp": "2026-08-26T12:10:00+00:00",
                "metadata": {"liquidity_added": "15", "liquidity_removed": "4"},
            },
            {
                "event_id": "taker-5m", "event_type": "TAKER_FLOW",
                "event_timestamp": "2026-08-26T12:10:00+00:00",
                "received_timestamp": "2026-08-26T12:10:00+00:00",
                "metadata": {"observationPeriod": "5m", "buyVol": "9", "sellVol": "4"},
            },
            {
                "event_id": "global-5m", "event_type": "GLOBAL_LONG_SHORT",
                "event_timestamp": "2026-08-26T12:10:00+00:00",
                "received_timestamp": "2026-08-26T12:10:00+00:00",
                "metadata": {"observationPeriod": "5m", "longShortRatio": "1.2"},
            },
            {
                "event_id": "top-5m", "event_type": "TOP_TRADER_LONG_SHORT",
                "event_timestamp": "2026-08-26T12:10:00+00:00",
                "received_timestamp": "2026-08-26T12:10:00+00:00",
                "metadata": {"observationPeriod": "5m", "longShortRatio": "1.3"},
            },
        ]

        features = bian_market.positioning_feature_values(events, as_of=as_of)

        self.assertEqual(features["cvd_1m"], Decimal("10"))
        self.assertEqual(features["cvd_5m"], Decimal("24"))
        self.assertEqual(features["cvd_acceleration"], Decimal("5"))
        self.assertTrue(features["price_cvd_divergence"])
        self.assertGreater(features["volume_ratio_1m"], Decimal("3"))
        self.assertGreater(features["volume_zscore"], Decimal("1"))
        self.assertEqual(features["price_impact_buy"], Decimal("0"))
        self.assertEqual(features["oi_change_5m"], Decimal("0.1"))
        self.assertEqual(features["oi_change"], Decimal("0.1"))
        self.assertEqual(features["liquidity_added"], Decimal("5"))
        self.assertEqual(features["liquidity_removed"], Decimal("1"))
        self.assertEqual(features["global_long_short_ratio"], Decimal("1.2"))
        self.assertEqual(features["top_trader_long_short_ratio"], Decimal("1.3"))

    def test_futures_observation_preserves_each_native_aggregate_period(self):
        class FakeFuturesClient:
            def get_mark_price(self, symbol: str):
                self.assert_symbol(symbol)
                return {"markPrice": "101", "indexPrice": "100", "time": 2_000}

            def get_open_interest(self, symbol: str):
                self.assert_symbol(symbol)
                return {"openInterest": "200", "time": 2_000}

            def get_funding_rate(self, symbol: str, *, limit: int):
                self.assert_symbol(symbol)
                assert limit == 2
                return [{"fundingRate": "0.0001", "fundingTime": 2_000}]

            def get_taker_buy_sell(self, symbol: str, *, period: str, limit: int):
                self.assert_symbol(symbol)
                assert limit == 2
                return [{"buyVol": "7", "sellVol": "5", "timestamp": 2_000}]

            def get_global_long_short_ratio(self, symbol: str, *, period: str, limit: int):
                self.assert_symbol(symbol)
                assert limit == 2
                return [{"longShortRatio": "1.1", "timestamp": 2_000}]

            def get_top_trader_long_short_ratio(self, symbol: str, *, period: str, limit: int):
                self.assert_symbol(symbol)
                assert limit == 2
                return [{"longShortRatio": "1.2", "timestamp": 2_000}]

            @staticmethod
            def assert_symbol(symbol: str):
                assert symbol == "BTCUSDT"

        report = bian_market.collect_futures_observations(
            ["BTCUSDT"], client=FakeFuturesClient()
        )

        period_events = [
            event for event in report["events"]
            if event["event_type"] in {
                "TAKER_FLOW", "GLOBAL_LONG_SHORT", "TOP_TRADER_LONG_SHORT",
            }
        ]
        self.assertEqual(len(report["events"]), 12)
        self.assertEqual({event["metadata"]["observationPeriod"] for event in period_events}, {"5m", "15m", "1h"})
        self.assertEqual(len({event["event_id"] for event in report["events"]}), len(report["events"]))

    def test_local_order_book_requires_contiguous_updates(self):
        book = bian_market.LocalOrderBook.from_snapshot(
            {"lastUpdateId": 10, "bids": [["100", "2"]], "asks": [["101", "3"]]}
        )
        self.assertTrue(book.apply_diff({"U": 11, "u": 11, "b": [["100", "4"]], "a": []}))
        with self.assertRaises(bian_market.OrderBookGap):
            book.apply_diff({"U": 13, "u": 13, "b": [], "a": []})

    def test_local_order_book_synchronizes_buffered_events_after_snapshot(self):
        book = bian_market.LocalOrderBook.synchronize(
            {"lastUpdateId": 10, "bids": [["100", "2"]], "asks": [["101", "3"]]},
            [{"U": 8, "u": 10, "b": [], "a": []}, {"U": 11, "u": 12, "b": [["100", "4"]], "a": []}],
        )
        self.assertEqual(book.last_update_id, 12)
        self.assertEqual(book.bids[Decimal("100")], Decimal("4"))

    def test_orderbook_gap_resynchronizes_from_rest_snapshot(self):
        books = {
            "BTC-USDT": bian_market.LocalOrderBook.from_snapshot(
                {"lastUpdateId": 10, "bids": [["100", "2"]], "asks": [["101", "3"]]}
            )
        }
        book = SimpleNamespace(symbol="BTC-USDT")
        gap = {"E": 1_786_493_002_000, "U": 13, "u": 13, "b": [["100", "4"]], "a": []}

        self.assertIsNone(
            bian_market._stream_orderbook(
                book, 1_786_493_002.0, raw=gap, books=books
            )
        )
        self.assertNotIn("BTC-USDT", books)

        snapshot = {"lastUpdateId": 12, "bids": [["100", "2"]], "asks": [["101", "3"]]}
        with patch.object(bian_market, "_get_json", return_value=snapshot) as get_json:
            recovered = bian_market._resynchronize_local_order_book(
                "BTC-USDT", [gap], books
            )

        self.assertEqual(recovered.last_update_id, 13)
        self.assertEqual(recovered.bids[Decimal("100")], Decimal("4"))
        self.assertIs(books["BTC-USDT"], recovered)
        self.assertEqual(recovered.snapshot_sync_origin, "REST_SNAPSHOT")
        self.assertEqual(recovered.snapshot_update_id, 12)
        self.assertIsNotNone(recovered.snapshot_received_timestamp)
        self.assertIn("/depth?symbol=BTCUSDT&limit=1000", get_json.call_args.args[0])

    def test_bookticker_and_orderbook_features_are_normalized(self):
        ticker = SimpleNamespace(
            symbol="BTC-USDT", bid=Decimal("100"), ask=Decimal("101"),
            timestamp=1_786_493_000.0, raw={"E": 1_786_493_000_000},
        )
        _, book_ticker = bian_market._stream_book_ticker(ticker, 1_786_493_001.0)
        books: dict[str, bian_market.LocalOrderBook] = {}
        book = SimpleNamespace(symbol="BTC-USDT")
        _, snapshot = bian_market._stream_orderbook(
            book,
            1_786_493_001.0,
            raw={"lastUpdateId": 10, "bids": [["100", "2"]], "asks": [["101", "3"]]},
            books=books,
        )
        _, diff = bian_market._stream_orderbook(
            book,
            1_786_493_002.0,
            raw={"E": 1_786_493_002_000, "U": 11, "u": 11, "b": [["100", "4"]], "a": []},
            books=books,
        )

        self.assertEqual(book_ticker["event_type"], "BOOK_TICKER")
        self.assertEqual(
            book_ticker["metadata"]["spreadBps"],
            str((Decimal("101") - Decimal("100")) / Decimal("100.5") * Decimal("10000")),
        )
        self.assertEqual(snapshot["event_type"], "ORDERBOOK")
        self.assertEqual(diff["metadata"]["bid_depth_5"], "4")
        self.assertIn("imbalance_5", diff["metadata"])
        self.assertIn("imbalance_20", diff["metadata"])
        self.assertEqual(diff["metadata"]["snapshotSyncOrigin"], "STREAM_SNAPSHOT")
        self.assertEqual(diff["metadata"]["snapshotLastUpdateId"], 10)
        self.assertEqual(books["BTC-USDT"].last_update_id, 11)

    def test_orderbook_gap_discards_local_book(self):
        books: dict[str, bian_market.LocalOrderBook] = {}
        book = SimpleNamespace(symbol="BTC-USDT")
        bian_market._stream_orderbook(
            book,
            1_786_493_001.0,
            raw={"lastUpdateId": 10, "bids": [["100", "2"]], "asks": [["101", "3"]]},
            books=books,
        )

        normalized = bian_market._stream_orderbook(
            book,
            1_786_493_002.0,
            raw={"E": 1_786_493_002_000, "U": 13, "u": 13, "b": [], "a": []},
            books=books,
        )

        self.assertIsNone(normalized)
        self.assertNotIn("BTC-USDT", books)

    def test_force_order_is_observation_with_explicit_side_semantics(self):
        liquidation = SimpleNamespace(
            symbol="BTC-USDT", side="SELL", price=Decimal("100"),
            quantity=Decimal("2"), timestamp=1_786_493_000.0, status="FILLED",
        )

        _, event = bian_market._stream_liquidation(liquidation, 1_786_493_001.0)

        self.assertEqual(event["market"], "FUTURES")
        self.assertEqual(event["event_type"], "FORCE_ORDER")
        self.assertEqual(event["notional"], "200")
        self.assertEqual(event["metadata"]["side_semantics"], "long_liquidation")

    def test_futures_liquidation_stream_is_public_only(self):
        from cryptofeed.symbols import Symbols

        async def callback(liquidation, receipt_timestamp):
            del liquidation, receipt_timestamp

        with patch.object(Symbols, "populated", return_value=True), patch.object(
            Symbols,
            "get",
            return_value=({"BTC-USDT": "BTCUSDT"}, None),
        ):
            handler = bian_market._build_futures_stream_handler(["BTC-USDT"], callback)
        feed = handler.feeds[0]
        self.assertEqual(feed.id, "BINANCE_FUTURES")
        self.assertFalse(feed.requires_authentication)

    def test_feed_handler_shutdown_uses_async_api_on_the_running_loop(self):
        class Handler:
            def __init__(self):
                self.loop = None

            async def stop_async(self, *, loop):
                self.loop = loop

        handler = Handler()

        async def shutdown() -> None:
            loop = asyncio.get_running_loop()
            await bian_market._shutdown_feed_handler(handler, loop)
            self.assertIs(handler.loop, loop)
            self.assertTrue(loop.is_running())

        asyncio.run(shutdown())

    def test_start_script_rejects_an_unrelated_listener(self):
        with socket.socket() as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", 18765))
            listener.listen()
            result = subprocess.run(
                ["bash", str(ROOT / "start_api.sh")],
                cwd=ROOT,
                env={
                    **__import__("os").environ,
                    "BIAN_API_PORT": "18765",
                    "BIAN_PYTHON_BIN": sys.executable,
                },
                text=True,
                capture_output=True,
                check=False,
            )

        self.assertEqual(result.returncode, 1)
        self.assertIn("non-bian service", result.stderr)

if __name__ == "__main__":
    unittest.main()

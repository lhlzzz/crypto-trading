from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest

from binance_client import BinanceAuthError, ClientConfig
from user_stream import UserStreamClient, normalize_user_event


def test_normalizes_execution_report() -> None:
    event = normalize_user_event(
        {
            "e": "executionReport",
            "E": 123,
            "s": "btcusdt",
            "c": "BIAN-ORDER-1",
            "i": 42,
            "X": "PARTIALLY_FILLED",
            "x": "TRADE",
            "z": "0.010",
            "l": "0.005",
            "L": "100.5",
            "n": "0.001",
            "N": "usdt",
        }
    )

    assert event.event_type == "executionReport"
    assert event.symbol == "BTCUSDT"
    assert event.client_order_id == "BIAN-ORDER-1"
    assert event.exchange_order_id == "42"
    assert event.order_status == "PARTIALLY_FILLED"
    assert event.executed_quantity == Decimal("0.010")
    assert event.last_price == Decimal("100.5")
    assert event.fee_asset == "USDT"


def test_normalizes_account_position() -> None:
    event = normalize_user_event(
        {
            "e": "outboundAccountPosition",
            "B": [{"a": "USDT", "f": "10", "l": "2"}],
        }
    )

    assert event.balances == ({"asset": "USDT", "free": "10", "locked": "2"},)


def test_user_stream_is_hard_blocked_in_paper_mode() -> None:
    with pytest.raises(BinanceAuthError, match="paper mode"):
        UserStreamClient(ClientConfig(mode="paper"))


def test_user_stream_connects_and_dispatches_reconciliation() -> None:
    class FakeStream:
        def __init__(self) -> None:
            self.callback = None

        def on(self, event, callback):
            assert event == "message"
            self.callback = callback

        async def unsubscribe(self):
            return None

    class FakeWebsocket:
        def __init__(self) -> None:
            self.callbacks = {}
            self.stream = FakeStream()

        async def create_connection(self):
            return None

        def on_connection(self, event, callback):
            self.callbacks[event] = callback

        async def user_data_stream_subscribe_signature(self):
            return SimpleNamespace(stream=self.stream)

        async def close_connection(self, **kwargs):
            return None

    ws = FakeWebsocket()
    sdk = SimpleNamespace(websocket_api=ws)
    events = []
    reconciled = []
    client = UserStreamClient(
        ClientConfig(mode="testnet", api_key="key", api_secret="secret"),
        on_event=events.append,
        on_reconcile=lambda: reconciled.append(True),
        sdk_factory=lambda config: sdk,
    )

    async def scenario():
        await client.connect_once()
        ws.stream.callback({"e": "executionReport", "s": "BTCUSDT", "X": "FILLED"})
        ws.callbacks["reconnect"]()
        await asyncio.sleep(0)

    asyncio.run(scenario())

    assert events[0].order_status == "FILLED"
    assert reconciled == [True]

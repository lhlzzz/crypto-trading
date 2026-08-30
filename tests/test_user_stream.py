from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from binance_client import BinanceAuthError, ClientConfig
from user_stream import UserStreamClient, normalize_user_event


def test_normalizes_order_trade_update() -> None:
    event = normalize_user_event(
        {
            "e": "ORDER_TRADE_UPDATE",
            "E": 123,
            "o": {
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
                "ps": "BOTH",
                "rp": "1.25",
                "R": True,
            },
        }
    )
    assert event.event_type == "ORDER_TRADE_UPDATE"
    assert event.symbol == "BTCUSDT"
    assert event.client_order_id == "BIAN-ORDER-1"
    assert event.exchange_order_id == "42"
    assert event.order_status == "PARTIALLY_FILLED"
    assert event.executed_quantity == Decimal("0.010")
    assert event.last_price == Decimal("100.5")
    assert event.fee_asset == "USDT"
    assert event.position_side == "BOTH"
    assert event.realized_pnl == Decimal("1.25")
    assert event.reduce_only is True
    assert event.event_id == "ORDER_TRADE_UPDATE:42:TRADE:0.010:123"


def test_normalizes_account_update() -> None:
    event = normalize_user_event(
        {
            "e": "ACCOUNT_UPDATE",
            "E": 9,
            "a": {
                "B": [{"a": "USDT", "wb": "100", "cw": "90", "bc": "1"}],
                "P": [{"s": "BTCUSDT", "pa": "0.1", "ep": "100", "up": "2", "mt": "isolated", "ps": "BOTH"}],
            },
        }
    )
    assert event.event_type == "ACCOUNT_UPDATE"
    assert event.balance_updates[0]["wallet_balance"] == "100"
    assert event.position_updates[0]["symbol"] == "BTCUSDT"
    assert event.position_updates[0]["quantity"] == "0.1"
    assert event.completeness == "PARTIAL"
    assert event.source == "USER_STREAM"


def test_spot_events_are_malformed_not_accepted() -> None:
    event = normalize_user_event({"e": "executionReport", "s": "BTCUSDT"})
    assert event.event_type == "malformed"


def test_unknown_user_events_are_malformed_not_accepted() -> None:
    event = normalize_user_event({"e": "unknownAccountEvent", "s": "BTCUSDT"})
    assert event.event_type == "malformed"


def test_malformed_event_does_not_raise() -> None:
    event = normalize_user_event("not-json")
    assert event.event_type == "malformed"


def test_user_stream_is_hard_blocked_in_paper_mode() -> None:
    with pytest.raises(BinanceAuthError, match="paper mode"):
        UserStreamClient(ClientConfig(mode="paper"))


class FakeRest:
    def __init__(self) -> None:
        self.created = 0
        self.closed = 0
        self.keepalives = 0

    def create_listen_key(self) -> str:
        self.created += 1
        return "listen-key"

    def keepalive_listen_key(self) -> None:
        self.keepalives += 1

    def close_listen_key(self) -> None:
        self.closed += 1


class FakeWebsocket:
    def __init__(self) -> None:
        self.messages = asyncio.Queue()
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.closed:
            raise StopAsyncIteration
        message = await self.messages.get()
        if message is None:
            raise StopAsyncIteration
        return message

    async def close(self):
        self.closed = True
        await self.messages.put(None)


def test_user_stream_dispatches_order_update_and_reconnect_reconcile() -> None:
    rest = FakeRest()
    sockets = [FakeWebsocket()]

    async def connect(url: str):
        assert "listen-key" in url
        return sockets[0]

    events = []
    reconciled = []
    halted = []
    client = UserStreamClient(
        ClientConfig(mode="testnet", api_key="key", api_secret="secret"),
        on_event=events.append,
        on_reconcile=lambda: reconciled.append(True),
        on_halt=lambda reason: halted.append(reason),
        rest_client=rest,
        websocket_connect=connect,
    )

    async def scenario():
        task = asyncio.create_task(client.run_forever())
        await asyncio.sleep(0)
        await sockets[0].messages.put(
            {
                "e": "ORDER_TRADE_UPDATE",
                "o": {"s": "BTCUSDT", "c": "BIAN-1", "X": "FILLED", "z": "0.1"},
            }
        )
        await asyncio.sleep(0.05)
        client._stopped = True
        await sockets[0].close()
        await asyncio.sleep(0.05)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())
    assert events[0].order_status == "FILLED"
    assert rest.created >= 1


def test_unknown_and_disconnect_trigger_reconciliation() -> None:
    rest = FakeRest()
    first = FakeWebsocket()
    calls = {"n": 0}

    async def connect(url: str):
        calls["n"] += 1
        if calls["n"] == 1:
            first.closed = True
            raise ConnectionError("disconnect")
        raise ConnectionError("still down")

    reconciled = []
    halted = []
    client = UserStreamClient(
        ClientConfig(mode="testnet", api_key="key", api_secret="secret"),
        on_reconcile=lambda: reconciled.append(True),
        on_halt=lambda reason: halted.append(reason),
        rest_client=rest,
        websocket_connect=connect,
        reconnect_delay_sec=0,
        max_failures=3,
    )

    async def scenario():
        with pytest.raises(ConnectionError):
            await asyncio.wait_for(client.run_forever(), timeout=2)

    asyncio.run(scenario())
    assert reconciled
    assert halted


def test_listen_key_expiry_reconnect() -> None:
    rest = FakeRest()
    first, second = FakeWebsocket(), FakeWebsocket()
    sockets = [first, second]

    async def connect(url: str):
        return sockets.pop(0)

    client = UserStreamClient(
        ClientConfig(mode="testnet", api_key="key", api_secret="secret"),
        rest_client=rest,
        websocket_connect=connect,
        reconnect_delay_sec=0,
    )

    async def scenario():
        task = asyncio.create_task(client.run_forever())
        await asyncio.sleep(0)
        await first.messages.put({"e": "listenKeyExpired", "E": 1})
        await asyncio.sleep(0.05)
        client._stopped = True
        await second.close()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())
    assert rest.created >= 2


def test_listen_key_expiry_halts_after_failures() -> None:
    rest = FakeRest()
    first, second = FakeWebsocket(), FakeWebsocket()
    sockets = [first, second]
    halted = []

    async def connect(url: str):
        return sockets.pop(0)

    client = UserStreamClient(
        ClientConfig(mode="testnet", api_key="key", api_secret="secret"),
        rest_client=rest,
        websocket_connect=connect,
        on_halt=halted.append,
        reconnect_delay_sec=0,
        max_failures=2,
    )

    async def scenario():
        task = asyncio.create_task(client.run_forever())
        await asyncio.sleep(0)
        await first.messages.put({"e": "listenKeyExpired", "E": 1})
        await asyncio.sleep(0.05)
        await second.messages.put({"e": "listenKeyExpired", "E": 2})
        with pytest.raises(RuntimeError, match="LISTEN_KEY_EXPIRED"):
            await task

    asyncio.run(scenario())
    assert halted


def test_listen_key_keepalive_runs() -> None:
    rest = FakeRest()
    socket = FakeWebsocket()

    async def connect(url: str):
        return socket

    client = UserStreamClient(
        ClientConfig(mode="testnet", api_key="key", api_secret="secret"),
        rest_client=rest,
        websocket_connect=connect,
        keepalive_sec=0.05,
    )

    async def scenario():
        task = asyncio.create_task(client.run_forever())
        await asyncio.sleep(0.12)
        client._stopped = True
        await socket.close()
        await asyncio.sleep(0.05)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())
    assert rest.keepalives >= 1


def test_listen_key_expired_triggers_reconnect_reconcile() -> None:
    event = normalize_user_event({"e": "listenKeyExpired"})
    assert event.event_type == "listenKeyExpired"

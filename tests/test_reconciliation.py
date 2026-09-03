from __future__ import annotations

from decimal import Decimal
from datetime import datetime, timezone
from types import SimpleNamespace

from reconciliation import Reconciler, apply_user_stream_event
from risk import FuturesAccountSnapshot
from user_stream import normalize_user_event


class StoreStub:
    def __init__(self, *, halted: bool = False, orders=None, balances=None, positions=None) -> None:
        self.halted = halted
        self.orders = orders or []
        self.balances = balances or []
        self.positions = positions or []
        self.halt_calls = []
        self.updated = []
        self.events = []
        self.trades = []

    def is_halted(self) -> bool:
        return self.halted

    def list_open_local_orders(self):
        return self.orders

    def list_balances(self):
        return self.balances

    def list_positions(self, **kwargs):
        return self.positions

    def get_balance(self, asset):
        return next((row for row in reversed(self.balances) if row["asset"] == asset), None)

    def get_position(self, symbol, **kwargs):
        return next((row for row in reversed(self.positions) if row["symbol"] == symbol), None)

    def set_halt(self, halted: bool, *, reason: str, source: str):
        self.halted = halted
        self.halt_calls.append((halted, reason, source))

    def upsert_balance(self, asset, **fields):
        self.balances.append({"asset": asset, **fields})

    def upsert_position(self, symbol, **fields):
        self.positions.append({"symbol": symbol, **fields})

    def record_trade(self, order_id, **fields):
        self.trades.append({"order_id": order_id, **fields})
        return order_id

    def update_order(self, order_id, **fields):
        self.updated.append((order_id, fields))

    def append_order_event(self, order_id, **fields):
        self.events.append((order_id, fields))

    def get_order_by_client_order_id(self, client_order_id):
        return next((row for row in self.orders if row["client_order_id"] == client_order_id), None)

    def record_system_event(self, **fields):
        self.events.append(fields)

    def record_risk_event(self, **fields):
        self.events.append(fields)


class ClientStub:
    def __init__(self, *, account=None, orders=None, positions=None, queried=None):
        self.account = account or {"assets": [{"asset": "USDT", "walletBalance": "100", "availableBalance": "100"}]}
        self.orders = orders or []
        self.positions = positions or []
        self.queried = queried or {}

    def get_account(self):
        return self.account

    def account_snapshot(self):
        assets = self.account.get("assets") or []
        usdt = next((row for row in assets if row.get("asset") == "USDT"), {})
        return FuturesAccountSnapshot(
            mode="testnet",
            wallet_balance=Decimal(str(usdt.get("walletBalance", "0"))),
            available_balance=Decimal(str(usdt.get("availableBalance", "0"))),
            total_margin=Decimal(str(usdt.get("walletBalance", "0"))),
            used_margin=Decimal("0"),
            unrealized_pnl=Decimal("0"),
            realized_pnl=Decimal("0"),
            positions=tuple(self.positions),
            open_orders=tuple(self.orders),
            leverage={
                str(row.get("symbol")): Decimal(str(row["leverage"]))
                for row in self.positions
                if row.get("symbol") and row.get("leverage") is not None
            },
            margin_mode="ISOLATED",
            position_mode="ONE_WAY",
            captured_at=datetime.now(timezone.utc),
            source="binance_futures_rest",
        )

    def get_open_orders(self, symbol=None):
        return self.orders

    def get_order(self, symbol, order_id=None, client_order_id=None):
        if client_order_id in self.queried:
            return self.queried[client_order_id]
        raise KeyError(client_order_id)

    def get_positions(self, symbol=None):
        return self.positions

    def get_user_trades(self, symbol, limit=None):
        return []


def test_paper_recovery_is_safe_without_unknown_orders() -> None:
    result = Reconciler(StoreStub(), mode="paper").recover()
    assert result.status == "SAFE"
    assert result.safe_to_trade is True


def test_matching_account_is_safe() -> None:
    store = StoreStub(
        balances=[{"asset": "USDT", "wallet_balance": "100", "free": "100", "mode": "testnet"}],
        positions=[{"symbol": "BTCUSDT", "quantity": "0.1", "position_side": "LONG", "entry_price": "100", "average_price": "100"}],
    )
    client = ClientStub(
        positions=[{"symbol": "BTCUSDT", "positionAmt": "0.1", "entryPrice": "100", "leverage": "2"}],
    )
    result = Reconciler(store, client=client, mode="testnet").recover()
    assert result.status == "SAFE"


def test_user_stream_preserves_binance_trade_id() -> None:
    store = StoreStub(
        orders=[
            {
                "order_id": "1",
                "client_order_id": "BIAN-1",
                "symbol": "BTCUSDT",
                "side": "BUY",
            }
        ]
    )
    event = normalize_user_event(
        {
            "e": "ORDER_TRADE_UPDATE",
            "E": 10,
            "o": {
                "s": "BTCUSDT",
                "c": "BIAN-1",
                "i": 9,
                "t": 77,
                "X": "FILLED",
                "x": "TRADE",
                "z": "0.1",
                "l": "0.1",
                "L": "100",
                "n": "0.01",
                "N": "USDT",
            },
        }
    )
    apply_user_stream_event(store, event)
    trade = next(item for item in store.trades if item.get("exchange_trade_id") == "77")
    assert trade["exchange_trade_id"] == "77"


def test_rest_user_trade_preserves_binance_trade_id() -> None:
    store = StoreStub(
        balances=[{"asset": "USDT", "wallet_balance": "100", "free": "100", "mode": "testnet"}],
        positions=[{"symbol": "BTCUSDT", "quantity": "0.1", "position_side": "LONG", "entry_price": "100", "average_price": "100"}],
        orders=[{
            "order_id": "1",
            "client_order_id": "c-1",
            "exchange_order_id": "9",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "status": "FILLED",
        }],
    )
    client = ClientStub(
        positions=[{"symbol": "BTCUSDT", "positionAmt": "0.1", "entryPrice": "100", "leverage": "2"}],
    )
    client.get_user_trades = lambda symbol, limit=None: [  # type: ignore[method-assign]
        {
            "id": 88,
            "orderId": 9,
            "symbol": "BTCUSDT",
            "side": "BUY",
            "qty": "0.1",
            "price": "100",
            "commission": "0.01",
            "commissionAsset": "USDT",
            "realizedPnl": "0",
        }
    ]
    Reconciler(store, client=client, mode="testnet").recover()
    trade = next(item for item in store.trades if item.get("exchange_trade_id") == "88")
    assert trade["exchange_trade_id"] == "88"


def test_account_update_testnet_does_not_mutate_live_position(monkeypatch) -> None:
    monkeypatch.setenv("BIAN_MODE", "testnet")
    store = StoreStub(
        positions=[
            {
                "symbol": "BTCUSDT",
                "quantity": "1",
                "position_side": "LONG",
                "entry_price": "100",
                "mode": "live",
            }
        ]
    )
    original_get = store.get_position

    def get_position(symbol, **kwargs):
        mode = kwargs.get("mode")
        if mode == "live":
            return store.positions[0]
        return None

    store.get_position = get_position  # type: ignore[method-assign]
    event = normalize_user_event(
        {
            "e": "ACCOUNT_UPDATE",
            "E": 1,
            "a": {"P": [{"s": "BTCUSDT", "pa": "9", "ep": "100", "up": "0", "cr": "0", "mt": "isolated", "ps": "BOTH"}]},
        }
    )
    apply_user_stream_event(store, event)
    live = next(row for row in store.positions if row.get("mode") == "live")
    assert str(live["quantity"]) == "1"
    assert store.positions[-1].get("mode") == "testnet"


def test_position_mismatch_halts() -> None:
    store = StoreStub(
        balances=[{"asset": "USDT", "wallet_balance": "100", "free": "100", "mode": "testnet"}],
        positions=[{"symbol": "BTCUSDT", "quantity": "0.2", "position_side": "LONG", "entry_price": "100", "average_price": "100"}],
    )
    client = ClientStub(
        positions=[{"symbol": "BTCUSDT", "positionAmt": "0.1", "entryPrice": "100"}],
    )
    result = Reconciler(store, client=client, mode="testnet").recover()
    assert result.status == "HALT"
    assert store.halt_calls


def test_unknown_exchange_order_fails_closed() -> None:
    store = StoreStub(
        orders=[{"status": "UNKNOWN", "client_order_id": "BIAN-UNKNOWN", "symbol": "BTCUSDT", "order_id": "1"}],
        balances=[{"asset": "USDT", "wallet_balance": "100", "free": "100", "mode": "testnet"}],
    )
    result = Reconciler(store, client=ClientStub(), mode="testnet").recover()
    assert result.status == "HALT"
    assert result.safe_to_trade is False
    assert store.halt_calls


def test_unknown_local_missing_exchange_order_halts() -> None:
    store = StoreStub(
        orders=[{"status": "ACKNOWLEDGED", "client_order_id": "BIAN-MISSING", "symbol": "BTCUSDT", "order_id": "1"}],
        balances=[{"asset": "USDT", "wallet_balance": "100", "free": "100", "mode": "testnet"}],
    )
    result = Reconciler(store, client=ClientStub(), mode="testnet").recover()
    assert result.status == "HALT"


def test_exchange_unknown_order_halts() -> None:
    store = StoreStub(
        orders=[{"status": "ACKNOWLEDGED", "client_order_id": "BIAN-1", "symbol": "BTCUSDT", "order_id": "1"}],
        balances=[{"asset": "USDT", "wallet_balance": "100", "free": "100", "mode": "testnet"}],
    )
    client = ClientStub(orders=[{"clientOrderId": "STRANGER", "status": "NEW"}])
    result = Reconciler(store, client=client, mode="testnet").recover()
    assert result.status == "HALT"
    assert "no local intent" in result.differences[0]


def test_user_stream_unknown_order_halts() -> None:
    store = StoreStub()
    event = normalize_user_event(
        {"e": "ORDER_TRADE_UPDATE", "o": {"c": "MISSING", "X": "FILLED", "s": "BTCUSDT"}}
    )
    apply_user_stream_event(store, event)
    assert store.halt_calls


def test_user_stream_duplicate_trade_event_is_idempotent() -> None:
    store = StoreStub(
        orders=[
            {
                "order_id": "1",
                "client_order_id": "BIAN-1",
                "symbol": "BTCUSDT",
                "side": "BUY",
            }
        ]
    )
    payload = {
        "e": "ORDER_TRADE_UPDATE",
        "E": 10,
        "o": {
            "s": "BTCUSDT",
            "c": "BIAN-1",
            "i": 9,
            "t": 77,
            "X": "FILLED",
            "x": "TRADE",
            "z": "0.1",
            "l": "0.1",
            "L": "100",
            "n": "0.01",
            "N": "USDT",
        },
    }
    event = normalize_user_event(payload)
    apply_user_stream_event(store, event)
    apply_user_stream_event(store, event)

    assert len(store.updated) == 1
    assert len(store.events) == 1


def test_user_stream_account_update_is_observation_only() -> None:
    store = StoreStub()
    event = normalize_user_event(
        {"e": "ACCOUNT_UPDATE", "a": {"B": [{"a": "USDT", "wb": "50", "cw": "40"}]}}
    )
    apply_user_stream_event(store, event)
    assert store.balances[0]["wallet_balance"] == Decimal("50")
    assert store.halt_calls == []


def test_account_update_patches_position_and_does_not_replace_unobserved_balance_state(monkeypatch) -> None:
    monkeypatch.setenv("BIAN_MODE", "testnet")
    store = StoreStub(
        balances=[
            {
                "asset": "USDT",
                "wallet_balance": "100",
                "available_balance": "80",
                "margin_balance": "99",
                "used_margin": "20",
                "mode": "testnet",
                "payload": {"funding_pnl": "-2"},
            }
        ],
        positions=[
            {
                "symbol": "BTCUSDT",
                "quantity": "1",
                "position_side": "LONG",
                "entry_price": "100",
                "mark_price": "101",
                "index_price": "100.5",
                "funding_pnl": "-2",
                "payload": {"keep": "yes"},
            }
        ],
    )
    event = normalize_user_event(
        {
            "e": "ACCOUNT_UPDATE",
            "a": {
                "B": [{"a": "USDT", "wb": "101", "cw": "81", "bc": "1"}],
                "P": [{"s": "BTCUSDT", "pa": "1.2", "ep": "100", "up": "3", "cr": "0", "mt": "isolated", "ps": "BOTH"}],
            },
        }
    )

    apply_user_stream_event(store, event)

    balance = store.balances[-1]
    assert balance["available_balance"] == Decimal("81")
    assert balance["used_margin"] == Decimal("20")
    position = store.positions[-1]
    assert position["position_side"] == "LONG"
    assert position["quantity"] == Decimal("1.2")
    assert position["mark_price"] == Decimal("101")


def test_short_position_uses_position_side_not_negative_quantity() -> None:
    store = StoreStub(
        balances=[{"asset": "USDT", "wallet_balance": "100", "free": "100", "mode": "testnet"}],
        positions=[{"symbol": "BTCUSDT", "quantity": "0.1", "position_side": "SHORT", "entry_price": "100", "average_price": "100"}],
    )
    client = ClientStub(
        positions=[{"symbol": "BTCUSDT", "positionAmt": "-0.1", "positionSide": "BOTH", "entryPrice": "100"}],
    )
    result = Reconciler(store, client=client, mode="testnet").recover()
    assert result.status == "SAFE"

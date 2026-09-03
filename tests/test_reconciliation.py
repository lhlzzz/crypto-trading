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

    def list_open_local_orders(self, *, mode=None, market="FUTURES"):
        rows = self.orders
        if mode is not None:
            rows = [row for row in rows if row.get("mode") in {None, mode}]
        pending = {
            "CREATED", "RISK_APPROVED", "SUBMITTED", "ACKNOWLEDGED",
            "NEW", "PENDING", "PARTIALLY_FILLED", "UNKNOWN",
        }
        return [row for row in rows if row.get("status") in pending or row.get("status") is None]

    def list_reconciliation_orders(self, *, mode, market="FUTURES", session_id=None):
        return [row for row in self.orders if row.get("mode") in {None, mode}]

    def list_orders(self, limit=50, *, mode=None, market="FUTURES"):
        rows = self.orders
        if mode is not None:
            rows = [row for row in rows if row.get("mode") in {None, mode}]
        return rows

    def list_trades(self, limit=50, *, mode=None, market="FUTURES"):
        rows = self.trades
        if mode is not None:
            rows = [row for row in rows if row.get("mode") in {None, mode}]
        return rows

    def list_balances(self, *args, **kwargs):
        mode = kwargs.get("mode")
        if mode is None:
            return self.balances
        return [row for row in self.balances if row.get("mode") in {None, mode}]

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
        exchange_trade_id = fields.get("exchange_trade_id")
        if exchange_trade_id is not None:
            for existing in self.trades:
                if str(existing.get("exchange_trade_id")) == str(exchange_trade_id):
                    return existing.get("order_id", order_id)
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
            "mode": "testnet",
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


def _safe_account_store(**kwargs):
    store_kwargs = {
        "balances": [{"asset": "USDT", "wallet_balance": "100", "free": "100", "mode": "testnet"}],
        "positions": [{
            "symbol": "BTCUSDT",
            "quantity": "0.1",
            "position_side": "LONG",
            "entry_price": "100",
            "average_price": "100",
        }],
    }
    store_kwargs.update(kwargs)
    return StoreStub(**store_kwargs)


def _trade_client(trades):
    client = ClientStub(
        positions=[{"symbol": "BTCUSDT", "positionAmt": "0.1", "entryPrice": "100", "leverage": "2"}],
    )
    client.get_user_trades = lambda symbol, limit=None: trades  # type: ignore[method-assign]
    return client


def test_unknown_exchange_trade_halts() -> None:
    store = _safe_account_store()
    client = _trade_client([
        {
            "id": 999,
            "orderId": 888,
            "symbol": "BTCUSDT",
            "side": "BUY",
            "qty": "0.1",
            "price": "100",
        }
    ])
    result = Reconciler(store, client=client, mode="testnet").recover()
    assert result.safe_to_trade is False
    assert result.status == "HALT"
    assert store.halt_calls
    assert "unknown exchange trade" in result.differences[0]
    assert "exchange_trade_id=999" in result.differences[0]
    assert "exchange_order_id=888" in result.differences[0]
    assert store.trades == []


def test_filled_local_order_recovers_binance_trade() -> None:
    store = _safe_account_store(orders=[{
        "order_id": "X",
        "client_order_id": "c-1",
        "exchange_order_id": "9",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "status": "FILLED",
        "mode": "testnet",
    }])
    client = _trade_client([
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
    ])
    reconciler = Reconciler(store, client=client, mode="testnet")
    result = reconciler.recover()
    assert result.status == "SAFE"
    assert result.safe_to_trade is True
    trade = next(item for item in store.trades if item.get("exchange_trade_id") == "88")
    assert trade["exchange_trade_id"] == "88"
    assert trade["order_id"] == "X"


def test_duplicate_reconciliation_does_not_duplicate_exchange_trade() -> None:
    store = _safe_account_store(orders=[{
        "order_id": "X",
        "client_order_id": "c-1",
        "exchange_order_id": "9",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "status": "FILLED",
        "mode": "testnet",
    }])
    client = _trade_client([
        {
            "id": 88,
            "orderId": 9,
            "symbol": "BTCUSDT",
            "side": "BUY",
            "qty": "0.1",
            "price": "100",
        }
    ])
    reconciler = Reconciler(store, client=client, mode="testnet")
    first = reconciler.recover()
    second = reconciler.recover()
    assert first.status == "SAFE"
    assert second.status == "SAFE"
    trades = [item for item in store.trades if item.get("exchange_trade_id") == "88"]
    assert len(trades) == 1


def test_reconciler_mode_overrides_environment_mode(monkeypatch) -> None:
    monkeypatch.setenv("BIAN_MODE", "live")
    store = _safe_account_store(orders=[
        {
            "order_id": "live-1",
            "client_order_id": "live-c",
            "exchange_order_id": "9",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "status": "FILLED",
            "mode": "live",
        },
        {
            "order_id": "tn-1",
            "client_order_id": "tn-c",
            "exchange_order_id": "9",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "status": "FILLED",
            "mode": "testnet",
        },
    ])
    client = _trade_client([
        {
            "id": 88,
            "orderId": 9,
            "symbol": "BTCUSDT",
            "side": "BUY",
            "qty": "0.1",
            "price": "100",
        }
    ])
    result = Reconciler(store, client=client, mode="testnet").recover()
    assert result.status == "SAFE"
    assert [item["order_id"] for item in store.trades] == ["tn-1"]
    assert all(item.get("mode") == "testnet" for item in store.trades)


def test_unknown_exchange_trade_is_not_swallowed_by_missing_loader() -> None:
    store = _safe_account_store()
    store.list_reconciliation_orders = None  # type: ignore[assignment]
    store.list_orders = None  # type: ignore[assignment]
    client = _trade_client([
        {
            "id": 999,
            "orderId": 888,
            "symbol": "BTCUSDT",
            "side": "BUY",
            "qty": "0.1",
            "price": "100",
        }
    ])
    result = Reconciler(store, client=client, mode="testnet").recover()
    assert result.status == "HALT"
    assert result.safe_to_trade is False
    assert store.halt_calls
    assert "unknown exchange trade" in result.differences[0]
    mismatch = [item for item in store.events if item.get("event_type") == "RECONCILIATION_MISMATCH"]
    assert mismatch


def test_missing_exchange_trade_id_fails_closed() -> None:
    store = _safe_account_store(orders=[{
        "order_id": "X",
        "client_order_id": "c-1",
        "exchange_order_id": "9",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "status": "FILLED",
        "mode": "testnet",
    }])
    client = _trade_client([
        {
            "orderId": 9,
            "symbol": "BTCUSDT",
            "side": "BUY",
            "qty": "0.1",
            "price": "100",
        }
    ])
    result = Reconciler(store, client=client, mode="testnet").recover()
    assert result.status == "HALT"
    assert result.safe_to_trade is False
    assert "missing exchange_trade_id" in result.differences[0]
    assert store.trades == []

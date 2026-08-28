from __future__ import annotations

from decimal import Decimal

from reconciliation import Reconciler


class StoreStub:
    def __init__(self, *, halted: bool = False, orders=None, balances=None) -> None:
        self.halted = halted
        self.orders = orders or []
        self.balances = balances or []
        self.halt_calls = []

    def is_halted(self) -> bool:
        return self.halted

    def list_open_local_orders(self):
        return self.orders

    def list_balances(self):
        return self.balances

    def set_halt(self, halted: bool, *, reason: str, source: str):
        self.halt_calls.append((halted, reason, source))


class ClientStub:
    def __init__(self, balances, orders):
        self.balances = balances
        self.orders = orders

    def get_account(self):
        return {"balances": self.balances}

    def get_open_orders(self, symbol=None):
        return self.orders

    def get_order(self, symbol, order_id=None, client_order_id=None):
        return {}


def test_paper_recovery_is_safe_without_unknown_orders() -> None:
    result = Reconciler(StoreStub(), mode="paper").recover()

    assert result.status == "SAFE"
    assert result.safe_to_trade is True


def test_unknown_exchange_order_fails_closed() -> None:
    store = StoreStub(
        orders=[
            {
                "status": "UNKNOWN",
                "client_order_id": "BIAN-UNKNOWN",
            }
        ],
        balances=[{"asset": "USDT", "free": "100", "mode": "testnet"}],
    )
    client = ClientStub(
        balances=[{"asset": "USDT", "free": "100"}],
        orders=[],
    )
    result = Reconciler(store, client=client, mode="testnet").recover()

    assert result.status == "HALT"
    assert result.safe_to_trade is False
    assert store.halt_calls


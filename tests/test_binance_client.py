from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from binance_sdk_spot import BadRequestError
from binance_sdk_spot.rest_api.models import NewOrderSideEnum, NewOrderTypeEnum

from binance_client import (
    BinanceAuthError,
    BinanceConnectionError,
    BinanceOrderError,
    BinanceRateLimitError,
    ClientConfig,
    FuturesPublicClient,
    PrivateClient,
    PublicClient,
    _translate_error,
)
from binance_sdk_spot import NetworkError, ServerError, TooManyRequestsError


def test_public_client_uses_public_sdk_without_credentials() -> None:
    with patch("binance_client.Spot") as spot:
        response = MagicMock()
        response.data.return_value = {"symbol": "BTCUSDT", "price": "100"}
        spot.return_value.rest_api.ticker_price.return_value = response
        client = PublicClient(ClientConfig(mode="paper"))

        assert client.get_ticker("BTCUSDT") == {
            "symbol": "BTCUSDT",
            "price": "100",
        }

    config = spot.call_args.kwargs["config_rest_api"]
    assert config.base_path == "https://api.binance.com"
    spot.return_value.rest_api.ticker_price.assert_called_once_with(
        symbol="BTCUSDT"
    )


def test_futures_public_client_retries_transient_timeout() -> None:
    response = MagicMock()
    response.read.return_value = b'{"markPrice":"100"}'
    response.__enter__.return_value = response
    client = FuturesPublicClient(
        ClientConfig(mode="paper", retries=1, backoff_ms=25)
    )

    with patch("binance_client.urllib.request.urlopen", side_effect=[
        TimeoutError("timed out"), response,
    ]) as urlopen, patch("binance_client.time.sleep") as sleep:
        payload = client.get_mark_price("BTCUSDT")

    assert payload == {"markPrice": "100"}
    assert urlopen.call_count == 2
    sleep.assert_called_once_with(0.025)


def test_private_client_is_hard_blocked_in_paper_mode() -> None:
    with pytest.raises(BinanceAuthError, match="paper mode"):
        PrivateClient(ClientConfig(mode="paper"))


def test_private_live_client_requires_explicit_enable() -> None:
    config = ClientConfig(
        mode="live",
        api_key="key",
        api_secret="secret",
        live_trading_enabled=False,
    )
    with pytest.raises(BinanceAuthError, match="LIVE_TRADING_ENABLED"):
        PrivateClient(config)


def test_private_live_client_requires_confirmation_token() -> None:
    config = ClientConfig(
        mode="live",
        api_key="key",
        api_secret="secret",
        live_trading_enabled=True,
    )
    with pytest.raises(BinanceAuthError, match="LIVE_CONFIRMATION_TOKEN"):
        PrivateClient(config)


def test_environment_credentials_are_mode_specific(monkeypatch) -> None:
    monkeypatch.setenv("BIAN_MODE", "testnet")
    monkeypatch.setenv("BIAN_API_KEY", "shared-key-must-not-be-used")
    monkeypatch.setenv("BIAN_API_SECRET", "shared-secret-must-not-be-used")
    monkeypatch.setenv("BIAN_TESTNET_API_KEY", "testnet-key")
    monkeypatch.setenv("BIAN_TESTNET_API_SECRET", "testnet-secret")

    testnet = ClientConfig.from_env()

    assert testnet.api_key == "testnet-key"
    assert testnet.api_secret == "testnet-secret"

    monkeypatch.setenv("BIAN_MODE", "live")
    monkeypatch.setenv("BIAN_LIVE_API_KEY", "live-key")
    monkeypatch.setenv("BIAN_LIVE_API_SECRET", "live-secret")
    live = ClientConfig.from_env()

    assert live.api_key == "live-key"
    assert live.api_secret == "live-secret"


def test_private_testnet_create_order_preserves_client_order_id() -> None:
    with patch("binance_client.Spot") as spot:
        response = MagicMock()
        response.data.return_value = {"orderId": 1}
        spot.return_value.rest_api.new_order.return_value = response
        client = PrivateClient(
            ClientConfig(
                mode="testnet",
                api_key="key",
                api_secret="secret",
            )
        )
        client.create_order(
            "BTCUSDT",
            "buy",
            "limit",
            quantity="0.01",
            price="100.00",
            client_order_id="BIAN-20260825-BTC-000001",
        )

    config = spot.call_args.kwargs["config_rest_api"]
    assert config.api_key == "key"
    assert config.api_secret == "secret"
    assert config.base_path == "https://testnet.binance.vision"
    spot.return_value.rest_api.new_order.assert_called_once_with(
        symbol="BTCUSDT",
        side=NewOrderSideEnum.BUY,
        type=NewOrderTypeEnum.LIMIT,
        quantity=0.01,
        quote_order_qty=None,
        price=100.0,
        new_client_order_id="BIAN-20260825-BTC-000001",
    )


def test_order_client_error_is_translated() -> None:
    with patch("binance_client.Spot") as spot:
        client = PrivateClient(
            ClientConfig(mode="testnet", api_key="key", api_secret="secret")
        )
        spot.return_value.rest_api.new_order.side_effect = BadRequestError(
            "Filter failure",
            400,
        )

        with pytest.raises(BinanceOrderError):
            client.create_order("BTCUSDT", "BUY", "MARKET", quantity="0.01")


@pytest.mark.parametrize(
    ("source", "translated"),
    [
        (TimeoutError("timeout"), BinanceConnectionError),
        (ConnectionError("reset"), BinanceConnectionError),
        (OSError("dns"), BinanceConnectionError),
        (NetworkError("network"), BinanceConnectionError),
        (ServerError("server", 503), BinanceConnectionError),
        (TooManyRequestsError("rate limit", 429), BinanceRateLimitError),
    ],
)
def test_transport_failures_use_unified_adapter_errors(source, translated) -> None:
    with pytest.raises(translated):
        raise _translate_error(source, operation="create_order")


def test_public_client_caches_and_normalizes_exchange_rules() -> None:
    with patch("binance_client.Spot") as spot:
        response = MagicMock()
        response.data.return_value = {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "TRADING",
                    "filters": [
                        {
                            "filterType": "PRICE_FILTER",
                            "tickSize": "0.10",
                        },
                        {
                            "filterType": "LOT_SIZE",
                            "minQty": "0.001",
                            "maxQty": "100",
                            "stepSize": "0.001",
                        },
                        {
                            "filterType": "MIN_NOTIONAL",
                            "minNotional": "5",
                        },
                    ],
                }
            ]
        }
        spot.return_value.rest_api.exchange_info.return_value = response
        client = PublicClient(ClientConfig(mode="paper"))

        assert client.get_symbol_rules("BTCUSDT") == {
            "symbol": "BTCUSDT",
            "status": "TRADING",
            "min_qty": "0.001",
            "max_qty": "100",
            "step_size": "0.001",
            "tick_size": "0.10",
            "min_notional": "5",
        }
        assert client.get_symbol_rules("BTCUSDT")["step_size"] == "0.001"
        spot.return_value.rest_api.exchange_info.assert_called_once_with(symbol=None)

from __future__ import annotations

import hashlib
import hmac
import io
import urllib.error
import urllib.parse
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from binance_client import (
    BinanceAuthError,
    BinanceAPIError,
    BinanceConnectionError,
    BinanceOrderError,
    BinanceRateLimitError,
    ClientConfig,
    FuturesPrivateClient,
    FuturesPublicClient,
    _translate_error,
)


def test_spot_public_client_is_removed() -> None:
    import binance_client

    assert not hasattr(binance_client, "PublicClient")
    assert not hasattr(binance_client, "SpotPublicClient")


def test_futures_public_client_retries_transient_timeout() -> None:
    response = MagicMock()
    response.read.return_value = b'{"markPrice":"100"}'
    response.__enter__.return_value = response
    client = FuturesPublicClient(
        ClientConfig(mode="paper", retries=1, backoff_ms=25)
    )

    with patch("scripts.bian_market.urllib.request.urlopen", side_effect=[
        TimeoutError("timed out"), response,
    ]) as urlopen, patch("binance_client.time.sleep") as sleep, patch(
        "binance_client.random.uniform", return_value=0
    ):
        payload = client.get_mark_price("BTCUSDT")

    assert payload == {"markPrice": "100"}
    assert urlopen.call_count == 2
    sleep.assert_called_once_with(0.025)


def test_futures_public_client_uses_shared_public_rest_opener() -> None:
    response = MagicMock()
    response.read.return_value = b'{"markPrice":"100"}'
    response.__enter__.return_value = response
    opener = MagicMock()
    opener.open.return_value = response
    client = FuturesPublicClient(ClientConfig(mode="paper"))

    with patch("binance_client._get_http_opener", return_value=opener) as get_opener:
        assert client.get_mark_price("BTCUSDT") == {"markPrice": "100"}

    get_opener.assert_called_once_with()
    opener.open.assert_called_once()


def test_futures_transport_records_runtime_metadata_and_retries() -> None:
    response = MagicMock()
    response.read.return_value = b'{"serverTime": 1}'
    response.__enter__.return_value = response
    opener = MagicMock()
    opener.open.side_effect = [TimeoutError("reset"), response]
    config = ClientConfig(mode="paper", retries=1, backoff_ms=25)

    with patch("binance_client._get_http_opener", return_value=opener), patch(
        "binance_client.time.sleep"
    ), patch("binance_client.random.uniform", return_value=0):
        assert config.transport_metadata is not None
        assert config.transport_metadata.proxy_mode in {"DIRECT", "CONFIGURED"}
        assert config.transport_metadata.as_dict()["last_error"] is None
        from binance_client import _futures_transport

        assert _futures_transport(
            config,
            "GET",
            "/fapi/v1/time",
            retries=1,
            operation="get_server_time",
        ) == {"serverTime": 1}

    metadata = config.transport_metadata
    assert metadata.connected_at is not None
    assert metadata.last_message_at is not None
    assert metadata.reconnect_count == 1
    assert metadata.transport_latency_ms is not None
    assert metadata.last_error is None


def test_futures_public_client_reads_aggregate_trades(monkeypatch) -> None:
    client = FuturesPublicClient(ClientConfig(mode="paper"))
    calls = []

    def fake_get(path, **params):
        calls.append((path, params))
        return [{"a": 1, "p": "100", "q": "2"}]

    monkeypatch.setattr(client, "_get", fake_get)

    assert client.get_aggregate_trades("BTCUSDT", limit=2000) == [
        {"a": 1, "p": "100", "q": "2"}
    ]
    assert calls == [
        (
            "https://fapi.binance.com/fapi/v1/aggTrades",
            {"symbol": "BTCUSDT", "limit": 1000},
        )
    ]


def test_futures_account_snapshot_uses_exchange_truth() -> None:
    client = FuturesPrivateClient(
        ClientConfig(mode="testnet", api_key="key", api_secret="secret")
    )
    with patch.object(
        client,
        "get_account",
        return_value={
            "totalMarginBalance": "120",
            "totalInitialMargin": "20",
            "totalUnrealizedProfit": "3",
            "totalRealizedProfit": "7",
            "assets": [{
                "asset": "USDT",
                "walletBalance": "100",
                "availableBalance": "80",
            }],
        },
    ), patch.object(
        client,
        "get_position_risk",
        return_value=[{
            "symbol": "DOGEUSDT",
            "positionAmt": "10",
            "leverage": "3",
            "marginType": "isolated",
        }],
    ), patch.object(
        client, "get_open_orders", return_value=[{"orderId": 1}]
    ), patch.object(
        client, "get_position_mode", return_value={"dualSidePosition": False}
    ):
        snapshot = client.account_snapshot()

    assert snapshot.mode == "testnet"
    assert snapshot.source == "binance_futures_rest"
    assert snapshot.wallet_balance == Decimal("100")
    assert snapshot.available_balance == Decimal("80")
    assert snapshot.used_margin == Decimal("20")
    assert snapshot.realized_pnl == Decimal("7")
    assert snapshot.leverage["DOGEUSDT"] == Decimal("3")
    assert snapshot.symbol_leverage["DOGEUSDT"] == Decimal("3")
    assert snapshot.position_mode == "ONE_WAY"
    assert snapshot.fresh is True


def test_futures_exchange_rules_zero_filters_fail_closed() -> None:
    client = FuturesPublicClient(ClientConfig(mode="testnet"))
    with patch.object(
        client,
        "get_exchange_info",
        return_value={
            "symbols": [{
                "symbol": "DOGEUSDT",
                "status": "TRADING",
                "quantityPrecision": 3,
                "filters": [{
                    "filterType": "LOT_SIZE",
                    "minQty": "0",
                    "stepSize": "0",
                }, {
                    "filterType": "PRICE_FILTER",
                    "tickSize": "0",
                }],
            }]
        },
    ):
        with pytest.raises(BinanceAPIError):
            client.get_symbol_rules("DOGEUSDT")


def test_private_client_is_hard_blocked_in_paper_mode() -> None:
    with pytest.raises(BinanceAuthError, match="paper mode"):
        FuturesPrivateClient(ClientConfig(mode="paper"))


def test_private_live_client_requires_explicit_enable() -> None:
    config = ClientConfig(
        mode="live",
        api_key="key",
        api_secret="secret",
        live_trading_enabled=False,
    )
    with pytest.raises(BinanceAuthError, match="LIVE_TRADING_ENABLED"):
        FuturesPrivateClient(config)


def test_private_live_client_requires_confirmation_token() -> None:
    config = ClientConfig(
        mode="live",
        api_key="key",
        api_secret="secret",
        live_trading_enabled=True,
    )
    with pytest.raises(BinanceAuthError, match="LIVE_CONFIRMATION_TOKEN"):
        FuturesPrivateClient(config)


def test_live_create_order_requires_matching_confirmation(monkeypatch) -> None:
    client = FuturesPrivateClient(
        ClientConfig(
            mode="live",
            api_key="key",
            api_secret="secret",
            live_trading_enabled=True,
            live_confirmation_token="secret",
        )
    )
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "true")
    monkeypatch.setenv("LIVE_CONFIRMATION_TOKEN", "secret")
    monkeypatch.setenv("BIAN_LIVE_CONFIRMATION", "other")
    with patch("scripts.bian_market.urllib.request.urlopen") as urlopen:
        with pytest.raises(BinanceAuthError, match="mismatch"):
            client.create_order("BTCUSDT", "BUY", "MARKET", quantity="0.01")
    assert urlopen.call_count == 0


def test_live_cancel_and_cancel_all_require_matching_confirmation(monkeypatch) -> None:
    client = FuturesPrivateClient(
        ClientConfig(
            mode="live",
            api_key="key",
            api_secret="secret",
            live_trading_enabled=True,
            live_confirmation_token="secret",
        )
    )
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "true")
    monkeypatch.setenv("LIVE_CONFIRMATION_TOKEN", "secret")
    monkeypatch.setenv("BIAN_LIVE_CONFIRMATION", "other")
    with patch("scripts.bian_market.urllib.request.urlopen") as urlopen:
        with pytest.raises(BinanceAuthError, match="mismatch"):
            client.cancel_order("BTCUSDT", order_id=1)
        with pytest.raises(BinanceAuthError, match="mismatch"):
            client.cancel_all_orders("BTCUSDT")
    assert urlopen.call_count == 0


def test_spot_private_client_is_removed() -> None:
    import binance_client
    assert not hasattr(binance_client, "PrivateClient")


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
    response = MagicMock()
    response.read.return_value = b'{"orderId":1,"status":"NEW"}'
    response.__enter__.return_value = response
    client = FuturesPrivateClient(
        ClientConfig(mode="testnet", api_key="key", api_secret="secret")
    )

    with patch("scripts.bian_market.urllib.request.urlopen", return_value=response) as urlopen, patch(
        "binance_client.time.time", return_value=1_700_000_000
    ):
        payload = client.create_order(
            "BTCUSDT",
            "buy",
            "limit",
            quantity="0.01",
            price="100.00",
            client_order_id="BIAN-20260825-BTC-000001",
        )

    request = urlopen.call_args.args[0]
    parsed = urllib.parse.urlparse(request.full_url)
    query = urllib.parse.parse_qs(parsed.query)
    unsigned, _, signature = parsed.query.rpartition("&signature=")
    expected = hmac.new(b"secret", unsigned.encode("utf-8"), hashlib.sha256).hexdigest()
    headers = {key.lower(): value for key, value in request.header_items()}

    assert payload == {"orderId": 1, "status": "NEW"}
    assert request.get_method() == "POST"
    assert parsed.scheme == "https"
    assert parsed.netloc == "testnet.binancefuture.com"
    assert parsed.path == "/fapi/v1/order"
    assert "testnet.binance.vision" not in request.full_url
    assert query["symbol"] == ["BTCUSDT"]
    assert query["side"] == ["BUY"]
    assert query["type"] == ["LIMIT"]
    assert query["quantity"] == ["0.01"]
    assert query["price"] == ["100.00"]
    assert query["timeInForce"] == ["GTC"]
    assert query["newClientOrderId"] == ["BIAN-20260825-BTC-000001"]
    assert "quoteOrderQty" not in query
    assert "reduceOnly" not in query
    assert signature == expected
    assert headers["x-mbx-apikey"] == "key"


def test_create_order_timeout_does_not_resubmit() -> None:
    client = FuturesPrivateClient(
        ClientConfig(mode="testnet", api_key="key", api_secret="secret", retries=3)
    )

    with patch(
        "scripts.bian_market.urllib.request.urlopen",
        side_effect=TimeoutError("timed out"),
    ) as urlopen:
        with pytest.raises(BinanceConnectionError):
            client.create_order("BTCUSDT", "BUY", "MARKET", quantity="0.01")

    assert urlopen.call_count == 1


def test_create_order_5xx_does_not_resubmit() -> None:
    client = FuturesPrivateClient(
        ClientConfig(mode="testnet", api_key="key", api_secret="secret", retries=3)
    )
    error = urllib.error.HTTPError(
        "https://testnet.binancefuture.com/fapi/v1/order",
        503,
        "Service Unavailable",
        hdrs=None,
        fp=io.BytesIO(b"temporarily unavailable"),
    )

    with patch("scripts.bian_market.urllib.request.urlopen", side_effect=error) as urlopen:
        with pytest.raises(BinanceConnectionError):
            client.create_order("BTCUSDT", "BUY", "MARKET", quantity="0.01")

    assert urlopen.call_count == 1


def test_unknown_order_can_be_resolved_by_get_order() -> None:
    client = FuturesPrivateClient(
        ClientConfig(mode="testnet", api_key="key", api_secret="secret", retries=3)
    )
    response = MagicMock()
    response.read.return_value = b'{"orderId":7,"status":"FILLED"}'
    response.__enter__.return_value = response

    with patch(
        "scripts.bian_market.urllib.request.urlopen",
        side_effect=[TimeoutError("timed out"), response],
    ) as urlopen:
        with pytest.raises(BinanceConnectionError):
            client.create_order(
                "BTCUSDT",
                "BUY",
                "MARKET",
                quantity="0.01",
                client_order_id="BIAN-UNKNOWN-1",
            )
        resolved = client.get_order("BTCUSDT", client_order_id="BIAN-UNKNOWN-1")

    assert resolved["status"] == "FILLED"
    assert urlopen.call_count == 2


def test_create_order_rejected_http_400_is_not_retried() -> None:
    client = FuturesPrivateClient(
        ClientConfig(mode="testnet", api_key="key", api_secret="secret", retries=3)
    )
    error = urllib.error.HTTPError(
        "https://testnet.binancefuture.com/fapi/v1/order",
        400,
        "Bad Request",
        hdrs=None,
        fp=io.BytesIO(b"Filter failure"),
    )

    with patch("scripts.bian_market.urllib.request.urlopen", side_effect=error) as urlopen:
        with pytest.raises(BinanceOrderError):
            client.create_order("BTCUSDT", "BUY", "MARKET", quantity="0.01")

    assert urlopen.call_count == 1


@pytest.mark.parametrize(
    ("method", "expected_path", "payload"),
    [
        ("create_listen_key", "/fapi/v1/listenKey", b'{"listenKey":"abc"}'),
        ("keepalive_listen_key", "/fapi/v1/listenKey", b"{}"),
        ("close_listen_key", "/fapi/v1/listenKey", b"{}"),
    ],
)
def test_listen_key_requests_are_api_key_only(
    method: str, expected_path: str, payload: bytes
) -> None:
    response = MagicMock()
    response.read.return_value = payload
    response.__enter__.return_value = response
    client = FuturesPrivateClient(
        ClientConfig(mode="testnet", api_key="key", api_secret="secret", retries=0)
    )

    with patch("scripts.bian_market.urllib.request.urlopen", return_value=response) as urlopen, patch(
        "binance_client.time.time", return_value=1_700_000_000
    ):
        result = getattr(client, method)()

    request = urlopen.call_args.args[0]
    parsed = urllib.parse.urlparse(request.full_url)
    query = urllib.parse.parse_qs(parsed.query)
    headers = {key.lower(): value for key, value in request.header_items()}
    assert request.get_method() == {
        "create_listen_key": "POST",
        "keepalive_listen_key": "PUT",
        "close_listen_key": "DELETE",
    }[method]
    assert parsed.path == expected_path
    assert headers["x-mbx-apikey"] == "key"
    assert "signature" not in query
    assert "timestamp" not in query
    assert "recvwindow" not in query
    if method == "create_listen_key":
        assert result == "abc"


def test_create_order_client_order_id_is_preserved_on_single_attempt() -> None:
    response = MagicMock()
    response.read.return_value = b'{"orderId":9,"status":"NEW"}'
    response.__enter__.return_value = response
    client = FuturesPrivateClient(
        ClientConfig(mode="testnet", api_key="key", api_secret="secret", retries=3)
    )

    with patch("scripts.bian_market.urllib.request.urlopen", return_value=response) as urlopen:
        client.create_order(
            "BTCUSDT", "BUY", "MARKET", quantity="0.01", client_order_id="BIAN-IDEMPOTENT-1"
        )

    request = urlopen.call_args.args[0]
    query = urllib.parse.parse_qs(urllib.parse.urlparse(request.full_url).query)
    assert query["newClientOrderId"] == ["BIAN-IDEMPOTENT-1"]
    assert urlopen.call_count == 1


def test_order_client_error_is_translated() -> None:
    client = FuturesPrivateClient(
        ClientConfig(mode="testnet", api_key="key", api_secret="secret", retries=0)
    )
    error = urllib.error.HTTPError(
        "https://testnet.binancefuture.com/fapi/v1/order",
        400,
        "Bad Request",
        hdrs=None,
        fp=io.BytesIO(b'{"code":-1111,"msg":"Filter failure"}'),
    )

    with patch("scripts.bian_market.urllib.request.urlopen", side_effect=error):
        with pytest.raises(BinanceOrderError, match="Filter failure"):
            client.create_order("BTCUSDT", "BUY", "MARKET", quantity="0.01")


def test_futures_private_create_order_requires_quantity() -> None:
    client = FuturesPrivateClient(
        ClientConfig(mode="testnet", api_key="key", api_secret="secret")
    )
    with pytest.raises(ValueError, match="quantity"):
        client.create_order("BTCUSDT", "BUY", "MARKET")


def test_futures_private_live_order_uses_production_host(monkeypatch) -> None:
    response = MagicMock()
    response.read.return_value = b'{"orderId":2}'
    response.__enter__.return_value = response
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "true")
    monkeypatch.setenv("LIVE_CONFIRMATION_TOKEN", "confirm")
    monkeypatch.setenv("BIAN_LIVE_CONFIRMATION", "confirm")
    client = FuturesPrivateClient(
        ClientConfig(
            mode="live",
            api_key="key",
            api_secret="secret",
            live_trading_enabled=True,
            live_confirmation_token="confirm",
        )
    )

    with patch("scripts.bian_market.urllib.request.urlopen", return_value=response) as urlopen:
        client.create_order("BTCUSDT", "SELL", "MARKET", quantity="0.01", reduce_only=True)

    request = urlopen.call_args.args[0]
    parsed = urllib.parse.urlparse(request.full_url)
    query = urllib.parse.parse_qs(parsed.query)
    assert parsed.netloc == "fapi.binance.com"
    assert query["side"] == ["SELL"]
    assert query["reduceOnly"] == ["true"]
    assert "quoteOrderQty" not in query


@pytest.mark.parametrize(
    ("source", "translated"),
    [
        (TimeoutError("timeout"), BinanceConnectionError),
        (ConnectionError("reset"), BinanceConnectionError),
        (OSError("dns"), BinanceConnectionError),
        (
            urllib.error.HTTPError(
                "https://fapi.binance.com/fapi/v1/order",
                400,
                "Bad Request",
                hdrs=None,
                fp=io.BytesIO(b"Filter failure"),
            ),
            BinanceOrderError,
        ),
    ],
)
def test_transport_failures_use_unified_adapter_errors(source, translated) -> None:
    with pytest.raises(translated):
        raise _translate_error(source, operation="create_order")


def test_futures_public_client_normalizes_exchange_rules() -> None:
    client = FuturesPublicClient(ClientConfig(mode="paper"))
    with patch.object(
        client,
        "get_exchange_info",
        return_value={
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "TRADING",
                    "quantityPrecision": 3,
                    "pricePrecision": 1,
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
        },
    ) as exchange_info:
        rules = client.get_symbol_rules("BTCUSDT")

    assert rules["symbol"] == "BTCUSDT"
    assert rules["status"] == "TRADING"
    assert rules["min_qty"] == "0.001"
    assert rules["max_qty"] == "100"
    assert rules["step_size"] == "0.001"
    assert rules["tick_size"] == "0.10"
    assert rules["min_notional"] == "5"
    exchange_info.assert_called_once_with("BTCUSDT")


def test_futures_private_create_order_rejects_unauthorized_symbol() -> None:
    client = FuturesPrivateClient(
        ClientConfig(mode="testnet", api_key="key", api_secret="secret")
    )
    with pytest.raises(BinanceOrderError, match="unauthorized symbol"):
        client.create_order("DOGEUSDT", "BUY", "MARKET", quantity="0.01")

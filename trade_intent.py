"""The strategy-to-risk contract for one proposed futures order."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

OrderType = Literal["MARKET", "LIMIT"]
Direction = Literal["LONG", "SHORT", "FLAT"]
Action = Literal["OPEN", "REDUCE", "CLOSE"]
MarginType = Literal["ISOLATED"]
PositionMode = Literal["ONE_WAY"]
ExchangeSide = Literal["BUY", "SELL"]
MemeRiskTier = Literal["TRADEABLE", "REDUCED", "OBSERVE", "BLOCK"]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _client_order_id(intent_id: UUID, symbol: str, created_at: datetime) -> str:
    date = created_at.astimezone(timezone.utc).strftime("%Y%m%d")
    compact_symbol = "".join(character for character in symbol.upper() if character.isalnum())
    return f"BIAN-{date}-{compact_symbol[:12]}-{intent_id.hex[:8].upper()}"


def exchange_side(direction: Direction, action: Action) -> ExchangeSide:
    """Map strategy semantics onto the Binance transport side.

    OPEN LONG -> BUY
    CLOSE/REDUCE LONG -> SELL
    OPEN SHORT -> SELL
    CLOSE/REDUCE SHORT -> BUY
    """
    if direction == "FLAT":
        raise ValueError("FLAT has no exchange side")
    if direction == "LONG":
        return "BUY" if action == "OPEN" else "SELL"
    return "SELL" if action == "OPEN" else "BUY"


class TradeIntent(BaseModel):
    """Immutable futures order intent produced by the strategy engine.

    This object contains no broker client and has no execution behavior.
    `side` is not a strategy field; adapters call exchange_side().
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    id: UUID = Field(default_factory=uuid4)
    symbol: str = Field(min_length=1, max_length=32)
    direction: Direction
    action: Action
    reduce_only: bool
    leverage: Decimal = Field(gt=0)
    margin_type: MarginType = "ISOLATED"
    position_mode: PositionMode = "ONE_WAY"
    order_type: OrderType
    quantity: Decimal = Field(gt=0)
    price: Decimal | None = Field(default=None, gt=0)
    confidence: Decimal = Field(ge=0, le=1)
    reason: str = Field(min_length=1, max_length=500)
    strategy_version: str = Field(min_length=1, max_length=100)
    created_at: datetime = Field(default_factory=_utc_now)
    client_order_id: str | None = Field(default=None, min_length=1, max_length=36)
    positioning_state: str | None = Field(default=None, max_length=64)
    previous_state: str | None = Field(default=None, max_length=64)
    transition: str | None = Field(default=None, max_length=128)
    long_score: Decimal | None = Field(default=None, ge=0, le=1)
    short_score: Decimal | None = Field(default=None, ge=0, le=1)
    crowding_score: Decimal | None = Field(default=None, ge=0, le=1)
    liquidity_score: Decimal | None = Field(default=None, ge=0, le=1)
    data_quality_score: Decimal | None = Field(default=None, ge=0, le=1)
    directional_strength: Decimal | None = Field(default=None, ge=0, le=1)
    transition_strength: Decimal | None = Field(default=None, ge=0, le=1)
    reason_codes: tuple[str, ...] = ()
    evidence_snapshot_id: UUID | None = None
    market_regime: str | None = Field(default=None, max_length=32)
    meme_risk_tier: MemeRiskTier | None = None

    def exchange_side(self) -> ExchangeSide:
        return exchange_side(self.direction, self.action)

    @model_validator(mode="after")
    def validate_order_contract(self) -> "TradeIntent":
        symbol = self.symbol.upper().strip()
        if not symbol:
            raise ValueError("symbol is required")
        object.__setattr__(self, "symbol", symbol)

        if self.direction == "FLAT":
            raise ValueError("direction=FLAT cannot open, reduce, or close")
        if self.action == "OPEN" and self.reduce_only:
            raise ValueError("OPEN requires reduce_only=false")
        if self.action in {"REDUCE", "CLOSE"} and not self.reduce_only:
            raise ValueError(f"{self.action} requires reduce_only=true")
        if self.margin_type != "ISOLATED":
            raise ValueError("margin_type must be ISOLATED")
        if self.position_mode != "ONE_WAY":
            raise ValueError("position_mode must be ONE_WAY")
        if self.order_type == "LIMIT" and self.price is None:
            raise ValueError("LIMIT intents require quantity and price")

        if self.created_at.tzinfo is None:
            object.__setattr__(self, "created_at", self.created_at.replace(tzinfo=timezone.utc))

        if self.client_order_id is None:
            object.__setattr__(
                self,
                "client_order_id",
                _client_order_id(self.id, self.symbol, self.created_at),
            )
        return self

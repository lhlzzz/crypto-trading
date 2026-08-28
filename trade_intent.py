"""The strategy-to-risk contract for one proposed order."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

Side = Literal["BUY", "SELL"]
OrderType = Literal["MARKET", "LIMIT"]
Direction = Literal["LONG", "SHORT", "FLAT"]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _client_order_id(intent_id: UUID, symbol: str, created_at: datetime) -> str:
    date = created_at.astimezone(timezone.utc).strftime("%Y%m%d")
    compact_symbol = "".join(character for character in symbol.upper() if character.isalnum())
    return f"BIAN-{date}-{compact_symbol[:12]}-{intent_id.hex[:8].upper()}"


class TradeIntent(BaseModel):
    """Immutable order intent produced by the strategy engine.

    This object contains no broker client and has no execution behavior.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    id: UUID = Field(default_factory=uuid4)
    symbol: str = Field(min_length=1, max_length=32)
    side: Side
    order_type: OrderType
    quantity: Decimal | None = Field(default=None, gt=0)
    quote_quantity: Decimal | None = Field(default=None, gt=0)
    price: Decimal | None = Field(default=None, gt=0)
    confidence: Decimal = Field(ge=0, le=1)
    reason: str = Field(min_length=1, max_length=500)
    strategy_version: str = Field(min_length=1, max_length=100)
    created_at: datetime = Field(default_factory=_utc_now)
    client_order_id: str | None = Field(default=None, min_length=1, max_length=36)
    direction: Direction | None = None
    positioning_state: str | None = Field(default=None, max_length=64)
    transition: str | None = Field(default=None, max_length=128)
    long_score: Decimal | None = Field(default=None, ge=0, le=1)
    short_score: Decimal | None = Field(default=None, ge=0, le=1)
    crowding_score: Decimal | None = Field(default=None, ge=0, le=1)
    liquidity_score: Decimal | None = Field(default=None, ge=0, le=1)
    data_quality_score: Decimal | None = Field(default=None, ge=0, le=1)
    reason_codes: tuple[str, ...] = ()
    evidence_snapshot_id: UUID | None = None
    market_regime: str | None = Field(default=None, max_length=32)

    @model_validator(mode="after")
    def validate_order_contract(self) -> "TradeIntent":
        symbol = self.symbol.upper().strip()
        if not symbol:
            raise ValueError("symbol is required")
        object.__setattr__(self, "symbol", symbol)

        if self.order_type == "LIMIT":
            if self.quantity is None or self.price is None:
                raise ValueError("LIMIT intents require quantity and price")
            if self.quote_quantity is not None:
                raise ValueError("LIMIT intents cannot use quote_quantity")
        elif self.quantity is None and self.quote_quantity is None:
            raise ValueError("MARKET intents require quantity or quote_quantity")
        elif self.quantity is not None and self.quote_quantity is not None:
            raise ValueError("provide quantity or quote_quantity, not both")

        if self.created_at.tzinfo is None:
            object.__setattr__(self, "created_at", self.created_at.replace(tzinfo=timezone.utc))

        if self.client_order_id is None:
            object.__setattr__(
                self,
                "client_order_id",
                _client_order_id(self.id, self.symbol, self.created_at),
            )
        return self

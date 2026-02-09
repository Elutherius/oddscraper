"""
Data models for Polymarket snapshot tool.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class MarketRecord:
    """Flattened market record for CSV output."""
    
    pulled_at_utc: str
    source: str
    market_id: str
    slug: str
    question: str
    category: str
    condition_id: str
    active: bool | None
    closed: bool | None
    end_date_utc: str
    outcomes_json: str
    clob_token_ids_json: str
    volume_num: float | None
    liquidity_num: float | None

    @staticmethod
    def csv_headers() -> list[str]:
        return [
            "pulled_at_utc",
            "source",
            "market_id",
            "slug",
            "question",
            "category",
            "condition_id",
            "active",
            "closed",
            "end_date_utc",
            "outcomes_json",
            "clob_token_ids_json",
            "volume_num",
            "liquidity_num",
        ]

    def to_csv_row(self) -> list[Any]:
        return [
            self.pulled_at_utc,
            self.source,
            self.market_id,
            self.slug,
            self.question,
            self.category,
            self.condition_id,
            self.active if self.active is not None else "",
            self.closed if self.closed is not None else "",
            self.end_date_utc,
            self.outcomes_json,
            self.clob_token_ids_json,
            self.volume_num if self.volume_num is not None else "",
            self.liquidity_num if self.liquidity_num is not None else "",
        ]


@dataclass
class TokenOutcome:
    """Mapping of token_id to its outcome label and parent market info."""
    
    token_id: str
    outcome: str
    market_id: str
    slug: str
    question: str
    active: bool | None
    volume_num: float | None
    liquidity_num: float | None


@dataclass
class PriceResult:
    """Price data for a single token outcome."""
    
    snapshot_ts_utc: str
    source: str
    market_id: str
    slug: str
    question: str
    token_id: str
    outcome: str
    bid: str  # Keep as string to preserve precision
    ask: str
    mid: str
    active: bool | None
    status: str  # "ok", "missing_price", "api_error"
    volume_num: float | None
    liquidity_num: float | None

    @staticmethod
    def csv_headers() -> list[str]:
        return [
            "snapshot_ts_utc",
            "source",
            "market_id",
            "slug",
            "question",
            "token_id",
            "outcome",
            "bid",
            "ask",
            "mid",
            "active",
            "status",
            "volume_num",
            "liquidity_num",
        ]

    def to_csv_row(self) -> list[Any]:
        return [
            self.snapshot_ts_utc,
            self.source,
            self.market_id,
            self.slug,
            self.question,
            self.token_id,
            self.outcome,
            self.bid,
            self.ask,
            self.mid,
            self.active if self.active is not None else "",
            self.status,
            self.volume_num if self.volume_num is not None else "",
            self.liquidity_num if self.liquidity_num is not None else "",
        ]


@dataclass
class RunManifest:
    """Run statistics and metadata."""
    
    start_ts_utc: str = ""
    end_ts_utc: str = ""
    duration_seconds: float = 0.0
    date: str = ""
    markets_total: int = 0
    markets_with_tokens: int = 0
    markets_skipped_no_tokens: int = 0
    markets_skipped_mismatched_arrays: int = 0
    markets_not_clob_tradable: int = 0
    tokens_total: int = 0
    tokens_priced_ok: int = 0
    tokens_missing_price: int = 0
    api_errors: int = 0
    price_batches: int = 0
    files: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "start_ts_utc": self.start_ts_utc,
            "end_ts_utc": self.end_ts_utc,
            "duration_seconds": self.duration_seconds,
            "date": self.date,
            "markets_total": self.markets_total,
            "markets_with_tokens": self.markets_with_tokens,
            "markets_skipped_no_tokens": self.markets_skipped_no_tokens,
            "markets_skipped_mismatched_arrays": self.markets_skipped_mismatched_arrays,
            "markets_not_clob_tradable": self.markets_not_clob_tradable,
            "tokens_total": self.tokens_total,
            "tokens_priced_ok": self.tokens_priced_ok,
            "tokens_missing_price": self.tokens_missing_price,
            "api_errors": self.api_errors,
            "price_batches": self.price_batches,
            "files": self.files,
        }

"""
Utility functions for time, chunking, JSON handling, and rate limiting.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Generator, TypeVar

T = TypeVar("T")


def utc_now() -> datetime:
    """Return current UTC datetime."""
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    """Return current UTC time as ISO string."""
    return utc_now().isoformat(timespec="seconds")


def utc_date_str(dt: datetime | None = None) -> str:
    """Return date as YYYY-MM-DD string."""
    if dt is None:
        dt = utc_now()
    return dt.strftime("%Y-%m-%d")


def chunk_list(items: list[T], size: int) -> Generator[list[T], None, None]:
    """Yield successive chunks of items."""
    for i in range(0, len(items), size):
        yield items[i : i + size]


def safe_json_dumps(obj: Any) -> str:
    """Stringify object to JSON, with fallback for edge cases."""
    try:
        return json.dumps(obj, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return json.dumps(str(obj))


def parse_json_string_field(value: Any) -> list | None:
    """
    Parse a field that may be a JSON-encoded string or already a list.
    
    Gamma API returns some fields (outcomes, clobTokenIds) as JSON strings.
    """
    if value is None:
        return None
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        value = value.strip()
        if value.startswith("["):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return parsed
            except json.JSONDecodeError:
                pass
    return None


def parse_decimal(value: Any) -> Decimal | None:
    """Parse a value as Decimal, returning None on failure."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def ensure_dirs(base_path: Path, date_str: str) -> dict[str, Path]:
    """
    Create all required directories and return paths dict.
    """
    paths = {
        "markets": base_path / "markets",
        "prices": base_path / "prices",
        "run": base_path / "run",
        "raw_gamma": base_path / "raw" / "gamma",
        "raw_clob_batches": base_path / "raw" / "clob" / "prices_batches" / f"markets_{date_str}",
    }
    for p in paths.values():
        p.mkdir(parents=True, exist_ok=True)
    return paths


class RateLimiter:
    """
    Thread-safe rate limiter with configurable requests per second.
    
    Usage:
        limiter = RateLimiter(requests_per_second=2.0)
        limiter.wait()  # blocks until allowed
        # make request
    """

    def __init__(self, requests_per_second: float = 1.0):
        self.min_interval = 1.0 / requests_per_second
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    def wait(self) -> None:
        """Block until the next request is allowed."""
        with self._lock:
            now = time.monotonic()
            if now < self._next_allowed:
                sleep_time = self._next_allowed - now
                time.sleep(sleep_time)
            self._next_allowed = time.monotonic() + self.min_interval

    def set_wait_until(self, seconds_from_now: float) -> None:
        """Set minimum wait time (for Retry-After handling)."""
        with self._lock:
            new_time = time.monotonic() + seconds_from_now
            if new_time > self._next_allowed:
                self._next_allowed = new_time

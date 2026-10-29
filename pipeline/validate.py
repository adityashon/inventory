
# pipeline/validate.py
from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

from datasource.csvdata.ingestion.csv_source import AcceptedRecord

logger = logging.getLogger(__name__)


class BusinessValidationError(Exception):
    """Raised when a row breaks one or more business rules."""

    def __init__(self, reasons: list[str]) -> None:
        super().__init__("; ".join(reasons))
        self.reasons = reasons


class Verify:
    def __init__(
        self,
        allowed_currency: set[str] | None = None,
        future_tolerance: timedelta = timedelta(minutes=5),
        max_quantity: int = 100_000,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.allowed_currency = allowed_currency
        self.future_tolerance = future_tolerance
        self.max_quantity = max_quantity
        self._now = now or (lambda: datetime.now(timezone.utc))

    def check(self, row: AcceptedRecord) -> list[str]:
        """Return every problem found. An empty list means the row is OK."""
        problems: list[str] = []

        if row.quantity <= 0:
            problems.append(f"quantity must be > 0, got {row.quantity}")
        elif row.quantity > self.max_quantity:
            problems.append(f"quantity {row.quantity} exceeds max {self.max_quantity}")

        if row.unit_price < 0:  # 0 is allowed (free / promo items)
            problems.append(f"unit_price is negative: {row.unit_price}")

        if row.order_date.tzinfo is None:
            problems.append("order_date must be timezone-aware")
        elif row.order_date > self._now() + self.future_tolerance:
            problems.append(f"order_date is in the future: {row.order_date.isoformat()}")

        if not row.currency:
            problems.append("currency is missing")
        elif self.allowed_currency is not None and row.currency not in self.allowed_currency:
            problems.append(f"currency {row.currency!r} not in allowed set")

        if not row.sku:
            problems.append("sku is missing")

        return problems

    def validate_row(self, row: AcceptedRecord) -> None:
        """Log and raise BusinessValidationError if the row has any problem."""
        problems = self.check(row)
        if problems:
            logger.error("order %s rejected: %s", row.order_id, "; ".join(problems))
            raise BusinessValidationError(problems)
# pipeline/deduplicate.py
from __future__ import annotations

from collections.abc import Callable, Hashable, Iterable, Iterator

from datasource.csvdata.ingestion.csv_source import AcceptedRecord


def default_key(row: AcceptedRecord) -> tuple:
    """
    generating unique composite key 
    """
    return (
        getattr(row, "source", None),
        row.order_id,
        getattr(row, "line_item_id", None) or row.sku,
    )


class Deduplicator:
    def __init__(self, key_fn: Callable[[AcceptedRecord], Hashable] = default_key) -> None:
        self.key_fn = key_fn
        self._seen: dict[Hashable, AcceptedRecord] = {}
        # (dropped_row, kept_row) for every duplicate found
        self.duplicates: list[tuple[AcceptedRecord, AcceptedRecord]] = []

    def is_duplicate(self, row: AcceptedRecord) -> bool:
        key = self.key_fn(row)
        if key in self._seen:
            self.duplicates.append((row, self._seen[key]))
            return True
        self._seen[key] = row
        return False

    def unique(self, rows: Iterable[AcceptedRecord]) -> Iterator[AcceptedRecord]:
        """Yield each row the first time its key is seen."""
        for row in rows:
            if not self.is_duplicate(row):
                yield row

    @property
    def conflicts(self) -> list[tuple[AcceptedRecord, AcceptedRecord]]:
        """Duplicates whose data differs from the kept row (e.g. a changed price)."""
        return [(dropped, kept) for dropped, kept in self.duplicates if dropped != kept]

    def reset(self) -> None:
        self._seen.clear()
        self.duplicates.clear()
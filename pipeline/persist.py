
from __future__ import annotations

import psycopg2

from datasource.csvdata.ingestion.csv_source import AcceptedRecord

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS sales_fact (
    source        TEXT          NOT NULL,
    order_id      TEXT          NOT NULL,
    line_item_id  TEXT          NOT NULL,
    sku           TEXT          NOT NULL,
    quantity      INTEGER       NOT NULL,
    order_date    TIMESTAMPTZ   NOT NULL,
    unit_price    NUMERIC(12,2) NOT NULL,
    currency      TEXT          NOT NULL,
    location_id   TEXT,
    loaded_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (source, order_id, line_item_id)
);
"""

UPSERT_SQL = """
INSERT INTO sales_fact
    (source, order_id, line_item_id, sku, quantity,
     order_date, unit_price, currency, location_id)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (source, order_id, line_item_id) DO UPDATE SET
    sku         = EXCLUDED.sku,
    quantity    = EXCLUDED.quantity,
    order_date  = EXCLUDED.order_date,
    unit_price  = EXCLUDED.unit_price,
    currency    = EXCLUDED.currency,
    location_id = EXCLUDED.location_id,
    loaded_at   = now();
"""


def to_params(row: AcceptedRecord) -> tuple:
    # Same fallbacks as default_key in deduplicate.py, so both agree on identity.
    return (
        getattr(row, "source", None) or "unknown",
        row.order_id,
        getattr(row, "line_item_id", None) or row.sku,
        row.sku,
        row.quantity,
        row.order_date,
        row.unit_price,
        row.currency,
        getattr(row, "location_id", None),
    )


class SalesLoader:
    def __init__(self, conn: psycopg2.Connection) -> None:
        self.conn = conn

    def ensure_table(self) -> None:
        self.conn.execute(CREATE_SQL)
        self.conn.commit()

    def upsert_batch(self, rows: list[AcceptedRecord]) -> None:
        """One batch = one transaction: all rows commit, or none do."""
        try:
            with self.conn.cursor() as cur:
                cur.executemany(UPSERT_SQL, [to_params(r) for r in rows])
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
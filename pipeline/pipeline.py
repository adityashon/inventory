
from __future__ import annotations
import logging
import os
from collections.abc import Iterable
from dataclasses import dataclass, field

from dotenv import load_dotenv

import psycopg2 ,os

from datasource.csvdata.ingestion.csv_source import AcceptedRecord
from pipeline.check_dup import Deduplicator
from pipeline.persist import SalesLoader
from pipeline.validate import Verify
load_dotenv
logger = logging.getLogger(__name__)

BATCH_SIZE = 500


@dataclass
class RunReport:
    fetched: int = 0
    upserted: int = 0
    duplicates: int = 0
    rejected: list[tuple[AcceptedRecord, list[str]]] = field(default_factory=list)

    def summary(self) -> str:
        return (f"fetched={self.fetched} upserted={self.upserted} "
                f"duplicates={self.duplicates} rejected={len(self.rejected)}")


def run_pipeline(
    records: Iterable[AcceptedRecord],
    loader: SalesLoader,
    verify: Verify,
    dedup: Deduplicator,
    batch_size: int = BATCH_SIZE,
) -> RunReport:
    report = RunReport()
    batch: list[AcceptedRecord] = []

    for row in records:                      # 1. fetch (records arrive one by one)
        report.fetched += 1

        problems = verify.check(row)         # 2. verify
        if problems:
            report.rejected.append((row, problems))
            logger.warning("order %s rejected: %s", row.order_id, "; ".join(problems))
            continue

        if dedup.is_duplicate(row):          # 3. deduplicate
            continue

        batch.append(row)                    # 4. persist, in batches
        if len(batch) >= batch_size:
            loader.upsert_batch(batch)
            report.upserted += len(batch)
            batch.clear()

    if batch:                                # the final partial batch
        loader.upsert_batch(batch)
        report.upserted += len(batch)

    report.duplicates = len(dedup.duplicates)
    logger.info("pipeline finished: %s", report.summary())
    return report


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        filename="pipeline.log",
        filemode="a",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # TODO: build your real sources here and combine their output, e.g.
    #   records = itertools.chain(amazon_source.fetch_sales(...), shopify_source.fetch_data())
    records: Iterable[AcceptedRecord] = []

    with psycopg2.connect(os.getenv["DATABASE_URL"]) as conn:
        loader = SalesLoader(conn)
        loader.ensure_table()
        report = run_pipeline(
            records,
            loader=loader,
            verify=Verify(allowed_currency={"USD", "INR"}),
            dedup=Deduplicator(),
        )
    print(report.summary())


if __name__ == "__main__":
    main()
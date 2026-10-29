

 

from datetime import datetime, timezone
from decimal import Decimal
from datasource.csvdata.ingestion.csv_source import AcceptedRecord
from pipeline.check_dup import Deduplicator

def make(order_id="1", sku="A", price="10"):
    return AcceptedRecord(order_id=order_id, sku=sku, quantity=1,
                          order_date=datetime.now(timezone.utc),
                          unit_price=Decimal(price), currency="USD")

d = Deduplicator()
rows = [make(), make(), make(price="12"), make(order_id="2")]
print(len(list(d.unique(rows))))   # 2 unique rows
print(len(d.duplicates))           # 2 duplicates dropped
print(len(d.conflicts))            # 1 (the price-12 one differs from the kept row)
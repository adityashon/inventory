from datetime import datetime, timedelta, timezone
from pipeline.validate import Verify
from datasource.csvdata.ingestion.csv_source import AcceptedRecord

v = Verify(allowed_currency={"USD", "INR"})
good = AcceptedRecord(order_id="1", sku="0901", quantity=10,
                      order_date=datetime.now(timezone.utc), unit_price=200, currency="USD")
print(v.check(good))  # []

bad = AcceptedRecord(order_id="2", sku="0901", quantity=10,
                     order_date=datetime.now(timezone.utc) + timedelta(days=3),
                     unit_price=200, currency="INR")
print(v.check(bad))   # two problems: future date and currency



bad = AcceptedRecord(order_id="2", sku="0901", quantity=10,
                     order_date=datetime.now(timezone.utc) + timedelta(days=3),
                     unit_price=-90, currency="XXX")
print(v.check(bad)) 
from datetime import datetime, timezone
from decimal import Decimal
import psycopg, os 
from dotenv import load_dotenv
from load_dotenv import load_dotenv
from datasource.csvdata.ingestion.csv_source import AcceptedRecord
from pipeline.pipeline import run_pipeline
from pipeline.persist import SalesLoader
from pipeline.validate import Verify
from pipeline.check_dup import Deduplicator
load_dotenv()
def make(order_id, sku="A", price="10"):
    return AcceptedRecord(order_id=order_id, sku=sku, quantity=1,
                          order_date=datetime.now(timezone.utc),
                          unit_price=Decimal(price), currency="USD")

rows = [make("1"), make("1"), make("2"), make("3", price="5")]  # 1 dup, 1 invalid

with psycopg.connect(os.getenv["DATABASE_URL"]) as conn:
    loader = SalesLoader(conn); loader.ensure_table()
    for _ in range(2):                       # run twice on purpose
        print(run_pipeline(rows, loader, Verify(), Deduplicator()).summary())
    print(conn.execute("SELECT count(*) FROM sales_fact").fetchone())  # expect (2,)
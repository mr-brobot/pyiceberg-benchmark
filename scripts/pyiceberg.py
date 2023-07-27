from datetime import datetime, date, time
from zoneinfo import ZoneInfo
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.expressions import GreaterThan

catalog = GlueCatalog("cloudbend")

table = catalog.load_table("benchmark.nyc_taxi")

# April 1st, 2023 in NYC timezone
pickup_datetime = datetime.combine(
    date(2023, 4, 1), time(0, 0, 0), tzinfo=ZoneInfo("America/New_York")
)

result = (
    table.scan(limit=10)
    .filter(GreaterThan("pickup_datetime", pickup_datetime.isoformat()))
    .to_arrow()
)

for record in result.to_pylist():
    print(record)

from pyiceberg.catalog.glue import GlueCatalog

catalog = GlueCatalog("cloudbend")

table = catalog.load_table("benchmark.nyc_taxi")

result = table.scan(limit=10).to_arrow()

assert len(result) == 10

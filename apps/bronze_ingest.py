import csv
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

# Directory that contains this script (resolved to an absolute path).
CURRENT_DIR = Path(__file__).resolve().parent

# Landing zone for raw files: <script dir>/../data/landing
LANDING_DIR = CURRENT_DIR.parent / "spark-data" / "landing"

# Path of csv files we generated
BASELINE = LANDING_DIR / "weather_baseline.csv"
INCREMENTALS = LANDING_DIR / "weather_incremental.csv"

NAMESPACE = "local.weather"
TABLE = f"{NAMESPACE}.bronze_weather"


def get_source_batch_id(path):

    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"Input file not found: {path}. Run generate_weather_data.py first."
        )

    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        first_row = next(reader, None)

        if first_row is None:
            raise ValueError(f"Input file has no data rows: {path}")

        batch_id = (first_row.get("source_batch_id") or "").strip()

        if not batch_id:
            raise ValueError(f"Missing source_batch_id in input file: {path}")

    return batch_id


# Discover batch IDs from the acutal input files
baseline_batch_id = get_source_batch_id(BASELINE)

incremental_batch_id = get_source_batch_id(INCREMENTALS)


# Create Spark Application
spark = SparkSession.builder.appName("WeatherAnalytics-Bronze").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")

# Find the source batches already represented in Bronze
loaded = set()

if spark.catalog.tableExists(TABLE):
    loaded = {
        row.source_batch_id
        for row in (spark.table(TABLE).select("source_batch_id").distinct().collect())
    }

# Load the baseline first, then the incremental batch
if baseline_batch_id not in loaded:
    path = BASELINE
elif incremental_batch_id not in loaded:
    path = INCREMENTALS
else:
    print("Both input batches are already in Bronzel; no incrementals made.")
    spark.stop()
    raise SystemExit(0)


# Read the selected CSV into a Spark DataFrame
events = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(str(path))
    .withColumn("observed_at", to_timestamp("observed_at"))
    .withColumn("ingested_at", to_timestamp("ingested_at"))
)

batch_id = events.select("source_batch_id").first()[0]


# Create the table on the first load; append subsequent batches
if not spark.catalog.tableExists(TABLE):
    events.writeTo(TABLE).using("iceberg").create()
else:
    events.writeTo(TABLE).append()


print(f"Loaded {batch_id}: {events.count():,} events")
print(f"Bronze total: {spark.table(TABLE).count():,}")


spark.stop()

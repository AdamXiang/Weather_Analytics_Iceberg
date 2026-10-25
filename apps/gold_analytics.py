"""
Build the Gold daily weather summary and export it for Power BI.

Medallion-architecture step: Silver (one current row per observation) ->
Gold (business-level aggregates, one row per station per day).

Processing steps:
    1. Read the Silver table.
    2. Aggregate observations per (station_id, observation_date):
         - observation_count:      number of observations that day,
         - avg_temperature_c:      mean temperature (2 decimals),
         - avg_humidity_pct:       mean relative humidity (2 decimals),
         - total_precipitation_mm: summed precipitation (2 decimals),
         - max_wind_speed_kmh:     highest wind speed (2 decimals).
    3. Write to the Gold Iceberg table:
         - first run: create the table from the aggregates;
         - later runs: MERGE (upsert) so that only days whose aggregates
           actually changed are updated and new days are inserted. This keeps
           the job idempotent and handles late/corrected Silver data.
    4. Export the full Gold table, sorted by date and station, as CSV with a
       header row for Power BI. The files are written in parallel by the
       Spark executors, so the driver never holds the whole table in memory.

Tables:
    SILVER: ``local.weather.silver_weather``      - input.
    GOLD:   ``local.weather.gold_daily_summary``  - output.

Output:
    A directory ``<script dir>/../spark-data/powerbi/daily_weather_summary/``
    containing one or more ``part-*.csv`` files (plus Spark marker files such
    as ``_SUCCESS``). In Power BI, use the "Folder" connector to combine them.
"""

from pathlib import Path

from pyspark.sql import SparkSession

# Import the functions module under an alias (instead of importing ``round``,
# ``sum`` and ``max`` by name) so the Python built-ins are not shadowed.
from pyspark.sql import functions as F

# Fully qualified names (catalog.namespace.table) of the input and output tables.
SILVER = "local.weather.silver_weather"
GOLD = "local.weather.gold_daily_summary"

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

# Directory that contains this script (resolved to an absolute path).
CURRENT_DIR = Path(__file__).resolve().parent

# Export directory for analytics/BI files: <script dir>/../spark-data/powerbi
ANALYTICS_DIR = CURRENT_DIR.parent / "spark-data" / "powerbi"

# Output directory for the CSV part files generated for Power BI.
# (Spark writes a directory of part files, not a single CSV file.)
SUMMARY_DIR = ANALYTICS_DIR / "daily_weather_summary"


# Create (or reuse) the Spark session and reduce console noise to errors only.
spark = SparkSession.builder.appName("WeatherAnalytics-Gold").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Aggregate Silver observations into one row per station per calendar day.
daily = (
    spark.table(SILVER)
    # Derive the calendar date (drops the time part) from the timestamp.
    .withColumn("observation_date", F.to_date("observed_at"))
    .groupBy("station_id", "observation_date")
    .agg(
        # Number of observations in the day (24 for a complete hourly day).
        F.count("event_id").alias("observation_count"),
        # Mean temperature and humidity, rounded to 2 decimals.
        F.round(F.avg("temperature_c"), 2).alias("avg_temperature_c"),
        F.round(F.avg("humidity_pct"), 2).alias("avg_humidity_pct"),
        # Daily precipitation total and peak wind speed.
        F.round(F.sum("precipitation_mm"), 2).alias("total_precipitation_mm"),
        F.round(F.max("wind_speed_kmh"), 2).alias("max_wind_speed_kmh"),
    )
)

# Expose the aggregates to Spark SQL so the MERGE statement can reference them.
daily.createOrReplaceTempView("daily_updates")


if not spark.catalog.tableExists(GOLD):
    # First run: the Gold table does not exist yet, so create it directly
    # from the aggregates (Iceberg format).
    daily.writeTo(GOLD).using("iceberg").create()
else:
    # Subsequent runs: upsert into the existing Gold table.
    #   t = target (Gold), s = source (the "daily_updates" view).
    spark.sql(f"""
        MERGE INTO {GOLD} t USING daily_updates s
        -- A Gold row is identified by its station and calendar day.
        ON t.station_id = s.station_id AND t.observation_date = s.observation_date
        -- Existing day: update only if at least one aggregate has changed,
        -- which avoids rewriting unchanged rows. ``<=>`` is the NULL-safe
        -- equality operator, so a change to/from NULL is also detected.
        WHEN MATCHED AND 
            (NOT (t.observation_count <=> s.observation_count) OR
            NOT (t.avg_temperature_c <=> s.avg_temperature_c) OR
            NOT (t.avg_humidity_pct <=> s.avg_humidity_pct) OR
            NOT (t.total_precipitation_mm <=> s.total_precipitation_mm) OR
            NOT (t.max_wind_speed_kmh <=> s.max_wind_speed_kmh))
        THEN UPDATE SET
            t.observation_count = s.observation_count,
            t.avg_temperature_c = s.avg_temperature_c,
            t.avg_humidity_pct = s.avg_humidity_pct,
            t.total_precipitation_mm = s.total_precipitation_mm,
            t.max_wind_speed_kmh = s.max_wind_speed_kmh
        -- New station/day combination: insert as-is.
        WHEN NOT MATCHED THEN INSERT *
    """)

# Read the full Gold table, sorted for stable, readable output.
gold = spark.table(GOLD).orderBy("observation_date", "station_id")

# Export to CSV with Spark so that the executors write the files in parallel
# and the driver never has to hold the whole table in memory.
#   - mode("overwrite"): replace the previous export, keeping reruns idempotent.
#   - header=True:       write the column names (taken from the table schema)
#                        as the first line of every part file.
# Spark creates the output directory (and any missing parents) itself and
# writes one or more ``part-*.csv`` files into it.
(gold.write.mode("overwrite").option("header", True).csv(str(SUMMARY_DIR)))


# Report the number of exported rows and the output location.
print(f"Gold rows: {gold.count():,}; Power BI folder: {SUMMARY_DIR}")

# Release Spark resources.
spark.stop()

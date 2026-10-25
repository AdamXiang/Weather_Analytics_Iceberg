"""
Build / refresh the Silver weather table from the Bronze layer.

Medallion-architecture step: Bronze (raw ingested events) -> Silver
(cleaned, de-duplicated, one current row per observation).

Processing steps:
    1. Read all rows from the Bronze table.
    2. Drop physically implausible readings (range validation).
    3. De-duplicate so that exactly one record remains per
       (station_id, observed_at), keeping the "latest" version, i.e. the one
       with the highest revision, then the latest ingestion time, then the
       largest event_id as a deterministic tie-breaker.
    4. Write to the Silver Iceberg table:
         - first run: create the table from the cleaned data;
         - later runs: MERGE (upsert) so that existing observations are only
           overwritten by strictly newer versions and unseen observations
           are inserted. This makes the job idempotent and safe to re-run.

Tables:
    BRONZE: ``local.weather.bronze_weather`` - raw events, may contain
        corrections (higher revision), late arrivals and duplicates.
    SILVER: ``local.weather.silver_weather`` - current observations.

Expected input columns:
    event_id, station_id, observed_at, ingested_at, temperature_c,
    humidity_pct, precipitation_mm, wind_speed_kmh, revision,
    source_batch_id.
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number

# Fully qualified names (catalog.namespace.table) of the source and target tables.
BRONZE = "local.weather.bronze_weather"
SILVER = "local.weather.silver_weather"

# Create (or reuse) the Spark session for this job.
spark = SparkSession.builder.appName("WeatherAnalytics-Silver").getOrCreate()
# Set the log level to ERROR prevent messy log info.
spark.sparkContext.setLogLevel("ERROR")


# Window used for de-duplication: all versions of the same observation
# (same station and observation time) are ranked from newest to oldest.
# Ranking order:
#   1. highest revision (a correction beats the original),
#   2. latest ingestion time (a later delivery beats an earlier one),
#   3. largest event_id (deterministic tie-breaker for exact duplicates).
latest = Window.partitionBy("station_id", "observed_at").orderBy(
    col("revision").desc(), col("ingested_at").desc(), col("event_id").desc()
)

# Build the cleaned, de-duplicated dataset ("updates") to apply to Silver.
updates = (
    spark.table(BRONZE)
    # Data-quality filter: keep only physically plausible readings.
    #   temperature: -80..60 degC, humidity: 0..100 %,
    #   precipitation: >= 0 mm,    wind speed: 0..400 km/h.
    # Note: the ``>= 0`` comparison is parenthesised because ``&`` binds
    # tighter than ``>=`` in Python.
    .filter(
        col("temperature_c").between(-80, 60)
        & col("humidity_pct").between(0, 100)
        & (col("precipitation_mm") >= 0)
        & col("wind_speed_kmh").between(0, 400)
    )
    # Rank every version of an observation; rank 1 is the most current one.
    .withColumn("rn", row_number().over(latest))
    # Keep only the most current version per (station_id, observed_at).
    .filter(col("rn") == 1)
    # The helper column is no longer needed.
    .drop("rn")
)


# Expose the DataFrame to Spark SQL so the MERGE statement can reference it.
updates.createOrReplaceTempView("updates")

if not spark.catalog.tableExists(SILVER):
    # First run: the Silver table does not exist yet, so create it directly
    # from the cleaned data (Iceberg format).
    updates.writeTo(SILVER).using("iceberg").create()
else:
    # Subsequent runs: upsert into the existing Silver table.
    #   t = target (Silver), s = source (the "updates" view).
    spark.sql(f"""
        MERGE INTO {SILVER} t USING updates s
        -- An observation is identified by its station and observation time.
        ON t.station_id = s.station_id AND t.observed_at = s.observed_at
        -- Existing observation: overwrite only if the source is newer, i.e.
        -- it has a higher revision, or the same revision but a later
        -- ingestion time. Older or identical data leaves the row unchanged.
        WHEN MATCHED 
                AND
                (s.revision > t.revision 
                OR 
                (s.revision = t.revision AND s.ingested_at > t.ingested_at))
        THEN UPDATE SET 
                t.event_id = s.event_id, 
                t.ingested_at = s.ingested_at,
                t.temperature_c = s.temperature_c,
                t.humidity_pct = s.humidity_pct,
                t.precipitation_mm = s.precipitation_mm,
                t.wind_speed_kmh = s.wind_speed_kmh,
                t.revision = s.revision,
                t.source_batch_id = s.source_batch_id
        -- New observation (late arrival or first time seen): insert as-is.
        WHEN NOT MATCHED THEN INSERT *
    """)

# Report how many current observations Silver now holds (sanity check).
print(f"Silver current observations: {spark.table(SILVER).count():,}")

# Release Spark resources.
spark.stop()

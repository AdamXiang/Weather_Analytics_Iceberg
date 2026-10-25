# Weather Analytics on Apache Iceberg

A local batch pipeline that turns synthetic hourly weather observations into a daily summary for Power BI. It runs on a Docker-based Spark standalone cluster and stores every layer as an Apache Iceberg table, following the medallion architecture (Bronze → Silver → Gold).

The project focuses on three problems that real ingestion pipelines face:

- **Corrections**: a later revision of an observation must replace the original.
- **Late-arriving events**: an observation that shows up after the fact must still be included.
- **Duplicate deliveries**: the same observation delivered twice must count once.

All jobs are idempotent. You can rerun any step and the tables converge to the same state.

## Architecture

```
generate_weather_data.py
        │  data/landing/weather_baseline.csv     (full year, revision 1)
        │  data/landing/weather_incremental.csv  (4 rows: correction, late event, 2 duplicates)
        ▼
bronze_ingest.py   ──►  local.weather.bronze_weather      raw events, append-only, one batch per run
        ▼
silver_upsert.py   ──►  local.weather.silver_weather      validated, de-duplicated, one row per observation
        ▼
gold_analytics.py  ──►  local.weather.gold_daily_summary  one row per station per day
                   ──►  data/powerbi/daily_weather_summary/part-*.csv
```

| Layer | Table | Write strategy |
| --- | --- | --- |
| Bronze | `local.weather.bronze_weather` | Append. Each run loads the next batch not yet present, detected by `source_batch_id`. |
| Silver | `local.weather.silver_weather` | `MERGE INTO` on `(station_id, observed_at)`. A row is updated only by a higher `revision`, or the same revision with a later `ingested_at`. |
| Gold | `local.weather.gold_daily_summary` | `MERGE INTO` on `(station_id, observation_date)`. A row is updated only when an aggregate changed. |

## Tech stack

- Apache Spark 3.5.1 (`apache/spark` Docker image), standalone mode: 1 master, 1 worker (2 cores, 2 GB)
- Apache Iceberg 1.9.2 (`iceberg-spark-runtime-3.5_2.12`), Hadoop catalog named `local`
- Python 3.13, managed with [uv](https://docs.astral.sh/uv/)
- Power BI (optional) for the dashboard on top of the Gold export

## Project structure

```
.
├── apps/                        # Spark applications, mounted at /opt/spark-apps
│   ├── generate_weather_data.py # synthetic data generator (runs on the host)
│   ├── bronze_ingest.py         # landing CSV -> Bronze
│   ├── silver_upsert.py         # Bronze -> Silver
│   └── gold_analytics.py        # Silver -> Gold + Power BI CSV export
├── data/                        # mounted at /opt/spark-data (git-ignored)
│   ├── landing/                 # generated CSV input
│   └── powerbi/                 # Gold CSV export
├── iceberg-warehouse/           # Iceberg tables, mounted at /opt/iceberg/warehouse (git-ignored)
├── jars/                        # Iceberg runtime JAR, mounted at /opt/iceberg-jars
├── docker-compose.yml           # Spark master + worker
├── spark_commands.txt           # annotated spark-submit / pyspark commands
└── pyproject.toml
```

## Prerequisites

- Docker with Docker Compose
- Python 3.13 and uv

## Getting started

### 1. Download the Iceberg runtime JAR

The JAR is not committed to the repository. Download it into `jars/`:

```bash
curl -L -o jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.9.2/iceberg-spark-runtime-3.5_2.12-1.9.2.jar
```

### 2. Start the Spark cluster

```bash
docker compose up -d
```

| UI | URL |
| --- | --- |
| Spark Master | http://localhost:8080 |
| Spark Worker | http://localhost:8081 |
| Spark Application (while a job runs) | http://localhost:4040 |

### 3. Generate the input data

The generator uses only the Python standard library and a fixed random seed, so the same year always produces the same files.

```bash
uv sync
uv run python apps/generate_weather_data.py 2024
```

If you omit the year, the script asks for one and defaults to 2025.

### 4. Run the pipeline

Every job is submitted with the same command. Replace `<script>` with the job name:

```bash
docker exec iceberg-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/iceberg-jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/opt/iceberg/warehouse/weather_analytics_project \
  --conf spark.sql.defaultCatalog=local \
  --conf spark.sql.session.timeZone=UTC \
  /opt/spark-apps/<script>.py
```

Run the jobs in this order:

1. `bronze_ingest` loads the baseline batch.
2. `silver_upsert` creates the Silver table.
3. `gold_analytics` creates the Gold table and the Power BI export.
4. `bronze_ingest` again loads the incremental batch.
5. `silver_upsert` again merges the correction, the late event, and the duplicates.
6. `gold_analytics` again updates only the affected days.

A third `bronze_ingest` run reports that both batches are already loaded and exits without changes.

`spark_commands.txt` explains every option in the command above.

## Expected results (year 2024)

2024 is a leap year: 8 stations × 8,784 hours = 70,272 baseline rows.

| Check | Value |
| --- | --- |
| Bronze rows after both batches | 70,276 (70,272 + 4) |
| Silver rows after both batches | 70,273 (the late event adds one; the correction and duplicates replace existing rows) |
| Gold rows | 2,928 (8 stations × 366 days) |
| `station-003` on 2024-06-15 | `observation_count = 25` (24 hourly + 1 late event at 12:30) |
| `station-001` on 2024-06-15 | Average temperature includes the revision 2 reading (+2.5 °C at 12:00) |

## How the edge cases are handled

| Scenario | Incremental row | Silver result |
| --- | --- | --- |
| A. Correction | `station-001`, 2024-06-15 12:00, `revision = 2` | Replaces the revision 1 row, because `s.revision > t.revision`. |
| B. Late event | `station-003`, 2024-06-15 12:30 | Inserted, because no row exists for that timestamp. |
| C. Duplicate delivery | `station-002`, 2024-06-20 08:00, delivered twice with different `event_id` | Collapsed to one row by the de-duplication window; it then replaces the baseline row because its `ingested_at` is later. The measurements stay the same. |

Before the merge, Silver also drops readings outside these ranges: temperature −80 to 60 °C, humidity 0 to 100 %, precipitation ≥ 0 mm, wind speed 0 to 400 km/h.

## Explore the tables

Open a PySpark shell inside the worker container:

```bash
docker exec -it iceberg-spark-worker bash

/opt/spark/bin/pyspark \
  --master spark://spark-master:7077 \
  --jars /opt/iceberg-jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/opt/iceberg/warehouse/weather_analytics_project
```

Then query the data and the Iceberg metadata:

```python
spark.table("local.weather.silver_weather").show()

# Snapshot history: one snapshot per create, append, or merge
spark.sql("SELECT committed_at, snapshot_id, operation FROM local.weather.silver_weather.snapshots").show()

# Time travel: read Silver as it was before the incremental merge
spark.sql("SELECT * FROM local.weather.silver_weather VERSION AS OF <snapshot_id>").show()
```

## Power BI

`gold_analytics.py` writes the full Gold table to `data/powerbi/daily_weather_summary/` as one or more `part-*.csv` files with a header row. In Power BI, use **Get Data → Folder**, select that directory, and combine the `part-*.csv` files.

## Reset

To start from an empty warehouse:

```bash
docker compose down
rm -rf iceberg-warehouse/weather_analytics_project data/powerbi
```

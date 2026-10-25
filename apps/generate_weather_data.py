"""
Generate deterministic, synthetic weather data for data-pipeline testing.

This script produces two CSV files in ``<project>/data/landing/``:

``weather_baseline.csv``
    One observation per station per hour for the chosen year. Every row is
    revision 1 and belongs to the batch ``baseline_<year>``.

``weather_incremental.csv``
    A tiny follow-up delivery (batch ``incremental_<year>``) that simulates
    three typical real-world ingestion problems:

    * Scenario A - Correction: a revised (revision 2) record that supersedes
      an existing baseline observation.
    * Scenario B - Late-arriving event: an observation for a timestamp that
      does not exist in the baseline, delivered after the fact.
    * Scenario C - Duplicate delivery: the same observation delivered twice
      under different ``event_id`` values.

The random seed is fixed, so repeated runs with the same year produce
identical files. All values are synthetic; no real weather API or
third-party dataset is used.

Usage:
    python generate_weather_data.py [YEAR]

If ``YEAR`` is omitted, the script prompts for it interactively and falls
back to ``DEFAULT_YEAR`` when the input is empty.

Output columns (see ``FIELDS``):
    event_id          Unique id of this delivery of the observation.
    station_id        Identifier of the weather station.
    observed_at       When the measurement was taken (event time).
    ingested_at       When the record arrived in the system (processing time).
    temperature_c     Air temperature in degrees Celsius.
    humidity_pct      Relative humidity in percent.
    precipitation_mm  Precipitation in millimetres.
    wind_speed_kmh    Wind speed in kilometres per hour.
    revision          Revision number of the observation (1 = original).
    source_batch_id   Identifier of the batch that delivered the record.
"""

import csv
import math
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

# Directory that contains this script (resolved to an absolute path).
CURRENT_DIR = Path(__file__).resolve().parent

# Landing zone for raw files: <script dir>/../data/landing
LANDING_DIR = CURRENT_DIR.parent / "data" / "landing"

# Create the landing directory (and any parents) if it does not exist yet.
LANDING_DIR.mkdir(parents=True, exist_ok=True)

# Output file paths: the full-year baseline and the small incremental delivery.
BASELINE = LANDING_DIR / "weather_baseline.csv"
INCREMENTALS = LANDING_DIR / "weather_incremental.csv"

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Fixed seed so every run with the same year yields exactly the same data.
random.seed(27)

# CSV column order, shared by the baseline and incremental files.
FIELDS = [
    "event_id",
    "station_id",
    "observed_at",
    "ingested_at",
    "temperature_c",
    "humidity_pct",
    "precipitation_mm",
    "wind_speed_kmh",
    "revision",
    "source_batch_id",
]

# Number of simulated weather stations (station-001 ... station-008).
STATION_COUNT = 8

# Year used when the user does not provide one.
DEFAULT_YEAR = 2025


def choose_year() -> int:
    """
    Determine the year for which weather data should be generated.

    The year is taken from the first command-line argument if present;
    otherwise the user is prompted interactively. An empty answer to the
    prompt selects ``DEFAULT_YEAR``.

    Returns:
        int: A validated year between 1900 and 2100 (inclusive).

    Raises:
        SystemExit: If the input is not an integer or lies outside the
            range 1900-2100. The exit message explains the problem.
    """
    if len(sys.argv) > 1:
        # Non-interactive mode: use the first CLI argument.
        value = sys.argv[1].strip()

    else:
        # Interactive mode: ask the user, offering the default value.
        value = input(
            f"Enter the year to generate weather data for (default: {DEFAULT_YEAR}): "
        ).strip()

        # Empty input means "use the default".
        if not value:
            value = str(DEFAULT_YEAR)

    # Convert to int, aborting with a friendly message on bad input.
    try:
        year = int(value)
    except ValueError:
        raise SystemExit(f"Invalid year: {value}. Please provide a valid integer year.")

    # Guard against unreasonable years.
    if year < 1900 or year > 2100:
        raise SystemExit(f"Year {year} is out of valid range (1900-2100).")

    return year


# Year selected for this run.
year = choose_year()

# Batch identifiers stamped on each row so the origin of a record is traceable.
BASELINE_BATCH_ID = f"baseline_{year}"
INCREMENTAL_BATCH_ID = f"incremental_{year}"

# Time window covered by the baseline: from the start of the year onward.
start_date = datetime(year, 1, 1)
end_date = datetime(year + 1, 1, 1)

# Number of calendar days and hourly timestamps in the window.
# ``days_in_year`` is also used as the period of the seasonal sine wave.
days_in_year = (end_date - start_date).days
hours_in_year = int((end_date - start_date).total_seconds() // 3600)


# ---------------------------------------------------------
# 1. Generate the baseline observations for the chosen year.
# ---------------------------------------------------------

# In-memory index of every baseline row, keyed by (station_id, observed_at).
# The incremental scenarios below reuse these rows as their starting point.
baseline = {}

with open(BASELINE, "w", newline="", encoding="utf-8") as f:
    # Write one CSV row per station per hour; header first.
    writer = csv.DictWriter(f, fieldnames=FIELDS)
    writer.writeheader()

    # Loop through each station and generate hourly weather data for the year
    for station_num in range(1, STATION_COUNT + 1):
        station_id = f"station-{station_num:03d}"

        for hour in range(hours_in_year):
            # Timestamp of the observation: start of year + N hours.
            observed_at = start_date + timedelta(hours=hour)

            # Seasonal component: one full sine cycle per year, shifted by
            # 80 days so the minimum falls in winter (amplitude 9 degC).
            seasonal = 9 * math.sin(
                2 * math.pi * (observed_at.timetuple().tm_yday - 80) / days_in_year
            )

            # Daily component: one cycle per 24 h, shifted by 6 h so the
            # temperature peaks in the afternoon (amplitude 5 degC).
            daily = 5 * math.sin(2 * math.pi * (observed_at.hour - 6) / 24)

            # Final temperature = base 13 degC
            #   + per-station offset (0.7 degC per station number)
            #   + seasonal + daily components
            #   + random noise in [-2, 2].
            temp = round(
                13 + station_num * 0.7 + seasonal + daily + random.uniform(-2, 2), 2
            )

            row = {
                # Unique id; "-r1" marks revision 1 of this observation.
                "event_id": (f"evt-{station_id}-{observed_at:%Y%m%dT%H%M%S}-r1"),
                "station_id": station_id,
                "observed_at": observed_at.strftime("%Y-%m-%d %H:%M:%S"),
                # Records are assumed to arrive 2 minutes after observation.
                "ingested_at": (observed_at + timedelta(minutes=2)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "temperature_c": temp,
                "humidity_pct": round(random.uniform(25, 95), 2),
                # Roughly 80% of hours are dry (4 of 5 choices are 0);
                # otherwise a light-to-moderate amount of 0.1-5 mm.
                "precipitation_mm": round(
                    random.choice([0, 0, 0, 0, random.uniform(0.1, 5)]),
                    2,
                ),
                "wind_speed_kmh": round(random.uniform(0, 45), 2),
                "revision": 1,
                "source_batch_id": BASELINE_BATCH_ID,
            }

            # Persist the row to the baseline CSV.
            writer.writerow(row)

            # Keep the row in memory, keyed by (station_id, observed_at),
            # so the incremental scenarios can look it up quickly.
            baseline[(station_id, observed_at)] = row


# ---------------------------------------------------------
# 2. Generate correction, late-arriving, and duplicate events.
# ---------------------------------------------------------

# Rows that will be written to the incremental file.
incrementals = []

# All incremental records arrive on Oct 1 at 10:00; individual scenarios add
# a few minutes on top so that their ingestion order is deterministic.
incrementals_ingested_at = datetime(year, 10, 1, 10, 0, 0)


# Scenario A: A correction to an existing observation.
# Copy the original baseline row for station-001 at Jun 15 12:00 and publish
# it as revision 2 with the temperature raised by 2.5 degC.
correction_time = datetime(year, 6, 15, 12, 0, 0)
key = ("station-001", correction_time)

corrected = baseline[key].copy()
corrected.update(
    # "-r2" marks this as the second revision of the same observation.
    event_id=f"evt-station-001-{correction_time:%Y%m%dT%H%M%S}-r2",
    ingested_at=incrementals_ingested_at.strftime("%Y-%m-%d %H:%M:%S"),
    temperature_c=round(float(corrected["temperature_c"]) + 2.5, 2),
    revision=2,
    source_batch_id=INCREMENTAL_BATCH_ID,
)

incrementals.append(corrected)


# Scenario B: A late-arriving observation at a new timestamp.
# The 12:30 timestamp has no baseline counterpart (the baseline is hourly),
# so this is a genuinely new record that shows up 5 minutes into the batch.
late_time = datetime(year, 6, 15, 12, 30, 0)

incrementals.append(
    {
        "event_id": f"evt-station-003-{late_time:%Y%m%dT%H%M%S}-r1",
        # FIXME: the baseline uses "station-003" but this row uses "ST-003",
        # so it will not join to the same station. Likely unintentional.
        "station_id": "station-003",
        "observed_at": late_time.strftime("%Y-%m-%d %H:%M:%S"),
        "ingested_at": (incrementals_ingested_at + timedelta(minutes=5)).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        # Hand-picked fixed values (not random) for this scenario.
        "temperature_c": 21.5,
        "humidity_pct": 63.0,
        "precipitation_mm": 0.2,
        "wind_speed_kmh": 12.0,
        "revision": 1,
        "source_batch_id": INCREMENTAL_BATCH_ID,
    }
)


# Scenario C: The same observation delivered twice.
# Copy one baseline row for station-002 and emit it two times. The measurements
# are identical; only event_id and ingested_at differ, so a pipeline must
# deduplicate on (station_id, observed_at, revision) rather than on event_id.
duplicate_time = datetime(year, 6, 20, 8, 0, 0)
key = ("station-002", duplicate_time)

# (event_id suffix, minutes after the batch start at which it was ingested)
for label, minutes_later in [("delivery-a", 6), ("delivery-b", 7)]:
    duplicate = baseline[key].copy()

    duplicate.update(
        event_id=(f"evt-station-002-{duplicate_time:%Y%m%dT%H%M%S}-{label}"),
        ingested_at=(
            incrementals_ingested_at + timedelta(minutes=minutes_later)
        ).strftime("%Y-%m-%d %H:%M:%S"),
        source_batch_id=INCREMENTAL_BATCH_ID,
    )

    incrementals.append(duplicate)


# Write all incremental rows (4 in total) to the incremental CSV.
with open(INCREMENTALS, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=FIELDS)

    writer.writeheader()
    writer.writerows(incrementals)


# ---------------------------------------------------------
# 3. Print a summary of what was generated.
# ---------------------------------------------------------

# Expected baseline size: one row per station per hour.
baseline_rows = STATION_COUNT * hours_in_year


print()
print(f"Generated weather data for {year}")
print(f"Calendar days: {days_in_year}")
print(f"Stations: {STATION_COUNT}")
print(f"Baseline: {BASELINE} ({baseline_rows:,} rows)")
print(
    f"Changes:  {INCREMENTALS} ({len(incrementals)} rows: "
    "correction, late event, duplicate deliveries)"
)
print(f"Baseline batch ID: {BASELINE_BATCH_ID}")
print(f"Changes batch ID:  {INCREMENTAL_BATCH_ID}")
print("All values are synthetic; no real weather API or third-party dataset was used.")

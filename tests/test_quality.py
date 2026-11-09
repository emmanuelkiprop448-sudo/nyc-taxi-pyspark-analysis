"""Each test row breaks exactly one data-quality rule (or none), so the counts are easy to check."""

from datetime import datetime, timedelta

import pytest
from pyspark.sql import SparkSession

from taxi_analysis.load import TRIP_COLUMNS
from taxi_analysis.quality import build_rules, flag_trips, month_bounds, summarise


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]").appName("tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def trip(pickup, minutes, distance=2.0, fare=12.0, passengers=1):
    start = datetime.fromisoformat(pickup)
    return (start, start + timedelta(minutes=minutes), passengers, distance, 161, 237, 1, fare, 2.0, 16.0)


# Mid-day, mid-month times so no row crosses a month boundary in any local time zone.
ROWS = {
    "good": trip("2024-01-10 08:00:00", 15),
    "good_missing_passengers": trip("2024-01-10 09:00:00", 15, passengers=None),
    "outside_month": trip("2023-12-15 12:00:00", 15),
    "bad_duration": trip("2024-01-10 10:00:00", 0.5, distance=0.2),
    "bad_distance": trip("2024-01-10 11:00:00", 15, distance=0.0),
    "bad_fare": trip("2024-01-10 12:00:00", 15, fare=-12.0),
    "bad_passengers": trip("2024-01-10 13:00:00", 15, passengers=0),
    "implausible_speed": trip("2024-01-10 14:00:00", 10, distance=40.0),
}


@pytest.fixture(scope="session")
def summary(spark):
    df = spark.createDataFrame(list(ROWS.values()), schema=", ".join(f"{c} {t}" for c, t in TRIP_COLUMNS.items()))
    rules = build_rules("2024-01")
    return summarise(flag_trips(df, rules), rules)


def test_month_bounds_wraps_year():
    assert month_bounds("2024-12") == (datetime(2024, 12, 1).date(), datetime(2025, 1, 1).date())


def test_totals(summary):
    assert summary["total"] == len(ROWS)
    assert summary["kept"] == 2  # the two "good" rows


@pytest.mark.parametrize("rule", ["outside_month", "bad_duration", "bad_distance", "bad_fare",
                                  "bad_passengers", "implausible_speed"])
def test_each_rule_catches_its_row(summary, rule):
    counts = {r["rule"]: r["failed"] for r in summary["rules"]}
    assert counts[rule] == 1


def test_missing_timestamps_counted(summary):
    counts = {r["rule"]: r["failed"] for r in summary["rules"]}
    assert counts["missing_timestamp"] == 0

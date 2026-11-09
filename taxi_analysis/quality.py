"""Data-quality rules for taxi trips.

Each rule is a condition that marks a trip as failing. Every trip gets one boolean
flag column per rule, so the report can show how many trips each rule caught
before the failing trips are removed. A trip can fail more than one rule.
"""

from dataclasses import dataclass
from datetime import date

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

PICKUP = "tpep_pickup_datetime"
DROPOFF = "tpep_dropoff_datetime"


@dataclass(frozen=True)
class Rule:
    name: str
    description: str
    fails: Column


def month_bounds(month: str) -> tuple[date, date]:
    """'2024-01' -> (2024-01-01, 2024-02-01)."""
    year, mon = (int(part) for part in month.split("-"))
    start = date(year, mon, 1)
    end = date(year + (mon == 12), mon % 12 + 1, 1)
    return start, end


def build_rules(month: str) -> list[Rule]:
    start, end = month_bounds(month)
    pickup, dropoff = F.col(PICKUP), F.col(DROPOFF)
    duration = F.col("duration_min")
    distance = F.col("trip_distance")
    fare = F.col("fare_amount")
    passengers = F.col("passenger_count")

    # coalesce(..., True): a missing value fails the rule.
    # coalesce(..., False): a missing value passes (used where null means "not recorded").
    return [
        Rule("missing_timestamp", "Pickup or drop-off time is missing",
             pickup.isNull() | dropoff.isNull()),
        Rule("outside_month", f"Pickup is outside {month}",
             F.coalesce(~((pickup >= F.lit(start)) & (pickup < F.lit(end))), F.lit(True))),
        Rule("bad_duration", "Trip lasts under 1 minute or over 3 hours (or ends before it starts)",
             F.coalesce((duration < 1) | (duration > 180), F.lit(True))),
        Rule("bad_distance", "Distance is zero, negative, missing or over 100 miles",
             F.coalesce((distance <= 0) | (distance > 100), F.lit(True))),
        Rule("bad_fare", "Fare is zero, negative (refunds and voids), missing or over $1,000",
             F.coalesce((fare <= 0) | (fare > 1000), F.lit(True))),
        Rule("bad_passengers", "Passenger count is 0 or more than 6 (missing counts are kept)",
             F.coalesce((passengers == 0) | (passengers > 6), F.lit(False))),
        Rule("implausible_speed", "Average speed is over 80 mph",
             F.coalesce(F.col("speed_mph") > 80, F.lit(False))),
    ]


def add_trip_metrics(df: DataFrame) -> DataFrame:
    """Duration in minutes and average speed in mph, needed by the rules."""
    seconds = F.unix_timestamp(F.col(DROPOFF)) - F.unix_timestamp(F.col(PICKUP))
    with_duration = df.withColumn("duration_min", seconds / 60.0)
    hours = F.col("duration_min") / 60.0
    speed = F.when(hours > 0, F.col("trip_distance") / hours)
    return with_duration.withColumn("speed_mph", speed)


def flag_trips(df: DataFrame, rules: list[Rule]) -> DataFrame:
    """Add one dq_<rule> column per rule and a dq_pass column that is true when no rule fails."""
    flagged = add_trip_metrics(df)
    for rule in rules:
        flagged = flagged.withColumn(f"dq_{rule.name}", rule.fails)
    any_fail = F.greatest(*[F.col(f"dq_{r.name}") for r in rules]) if len(rules) > 1 else F.col(f"dq_{rules[0].name}")
    return flagged.withColumn("dq_pass", ~any_fail)


def summarise(flagged: DataFrame, rules: list[Rule]) -> dict:
    """Count trips caught by each rule and the trips kept, in a single pass over the data."""
    aggs = [F.count(F.lit(1)).alias("total"), F.sum(F.col("dq_pass").cast("long")).alias("kept")]
    aggs += [F.sum(F.col(f"dq_{r.name}").cast("long")).alias(r.name) for r in rules]
    row = flagged.agg(*aggs).first()
    total = row["total"] or 0
    return {
        "total": total,
        "kept": row["kept"] or 0,
        "rules": [
            {"rule": r.name, "description": r.description, "failed": row[r.name] or 0,
             "failed_pct": round(100 * (row[r.name] or 0) / total, 2) if total else 0.0}
            for r in rules
        ],
    }

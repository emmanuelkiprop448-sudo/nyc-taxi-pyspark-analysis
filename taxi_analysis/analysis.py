"""Feature engineering and the Spark SQL queries behind each finding."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

PAYMENT_LABELS = """
    CASE payment_type
        WHEN 1 THEN 'Credit card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No charge'
        WHEN 4 THEN 'Dispute'
        ELSE 'Other or unknown'
    END
"""


def add_features(clean: DataFrame) -> DataFrame:
    """Columns used by the analysis queries."""
    return (
        clean.withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        # Spark's dayofweek is 1 = Sunday ... 7 = Saturday; shift so 1 = Monday ... 7 = Sunday.
        .withColumn("pickup_weekday", ((F.dayofweek("tpep_pickup_datetime") + 5) % 7) + 1)
        .withColumn("pickup_day_name", F.date_format("tpep_pickup_datetime", "EEE"))
        # Tips are only recorded for card payments; cash tips never reach the meter.
        .withColumn(
            "tip_pct",
            F.when((F.col("payment_type") == 1) & (F.col("fare_amount") > 0),
                   F.col("tip_amount") / F.col("fare_amount") * 100),
        )
    )


QUERIES = {
    # Average trips per day for each weekday and hour. Dividing by the number of
    # dates avoids favouring weekdays that occur five times in the month.
    "demand_by_weekday_hour": """
        SELECT pickup_weekday, pickup_day_name, pickup_hour,
               COUNT(*) AS trips,
               COUNT(DISTINCT pickup_date) AS days,
               ROUND(COUNT(*) / COUNT(DISTINCT pickup_date), 1) AS avg_trips_per_day
        FROM trips
        GROUP BY pickup_weekday, pickup_day_name, pickup_hour
        ORDER BY pickup_weekday, pickup_hour
    """,
    "top_pickup_zones": """
        SELECT z.Borough AS borough, z.Zone AS zone,
               COUNT(*) AS trips,
               ROUND(AVG(t.fare_amount), 2) AS avg_fare_usd,
               ROUND(AVG(t.trip_distance), 2) AS avg_distance_mi
        FROM trips t
        JOIN zones z ON t.PULocationID = z.LocationID
        GROUP BY z.Borough, z.Zone
        ORDER BY trips DESC
        LIMIT 15
    """,
    "borough_summary": """
        SELECT z.Borough AS pickup_borough,
               COUNT(*) AS trips,
               ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS share_pct,
               ROUND(SUM(t.fare_amount) / SUM(t.trip_distance), 2) AS fare_per_mile_usd,
               ROUND(percentile_approx(t.duration_min, 0.5), 1) AS median_duration_min
        FROM trips t
        JOIN zones z ON t.PULocationID = z.LocationID
        GROUP BY z.Borough
        ORDER BY trips DESC
    """,
    "tips_by_hour": f"""
        SELECT pickup_hour,
               COUNT(*) AS card_trips,
               ROUND(percentile_approx(tip_pct, 0.5), 1) AS median_tip_pct,
               ROUND(100 * AVG(CASE WHEN tip_amount = 0 THEN 1 ELSE 0 END), 1) AS no_tip_pct
        FROM trips
        WHERE payment_type = 1
        GROUP BY pickup_hour
        ORDER BY pickup_hour
    """,
    "payment_mix": f"""
        SELECT {PAYMENT_LABELS} AS payment,
               COUNT(*) AS trips,
               ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS share_pct
        FROM trips
        GROUP BY 1
        ORDER BY trips DESC
    """,
    # A trip counts as an airport trip if it starts or ends at JFK, LaGuardia or Newark.
    "airport_trips": """
        WITH tagged AS (
            SELECT t.*,
                   CASE WHEN pz.service_zone IN ('Airports', 'EWR') THEN pz.Zone END AS pickup_airport,
                   CASE WHEN dz.service_zone IN ('Airports', 'EWR') THEN dz.Zone END AS dropoff_airport
            FROM trips t
            LEFT JOIN zones pz ON t.PULocationID = pz.LocationID
            LEFT JOIN zones dz ON t.DOLocationID = dz.LocationID
        )
        SELECT COALESCE(pickup_airport, dropoff_airport) AS airport,
               COUNT(*) AS trips,
               ROUND(AVG(fare_amount), 2) AS avg_fare_usd,
               ROUND(AVG(total_amount), 2) AS avg_total_usd,
               ROUND(AVG(trip_distance), 1) AS avg_distance_mi
        FROM tagged
        WHERE COALESCE(pickup_airport, dropoff_airport) IS NOT NULL
        GROUP BY 1
        ORDER BY trips DESC
    """,
    "headline": """
        SELECT COUNT(*) AS trips,
               ROUND(AVG(fare_amount), 2) AS avg_fare_usd,
               ROUND(AVG(trip_distance), 2) AS avg_distance_mi,
               ROUND(percentile_approx(duration_min, 0.5), 1) AS median_duration_min,
               ROUND(100 * AVG(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END), 1) AS card_share_pct
        FROM trips
    """,
}


def run_queries(spark: SparkSession, trips: DataFrame, zones: DataFrame) -> dict:
    """Register the views and return each query's result as a pandas DataFrame (all results are small)."""
    trips.createOrReplaceTempView("trips")
    zones.createOrReplaceTempView("zones")
    return {name: spark.sql(sql).toPandas() for name, sql in QUERIES.items()}

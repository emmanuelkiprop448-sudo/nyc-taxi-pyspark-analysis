"""Spark session and data loading."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# The columns this project uses, with the type each is cast to.
# Parquet (official TLC files) and CSV (mirror files) both end up with this schema.
TRIP_COLUMNS = {
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "int",
    "trip_distance": "double",
    "PULocationID": "int",
    "DOLocationID": "int",
    "payment_type": "int",
    "fare_amount": "double",
    "tip_amount": "double",
    "total_amount": "double",
}


def get_spark(app_name: str = "nyc-taxi-analysis") -> SparkSession:
    """Local Spark session. Timestamps are kept as New York wall-clock time by using UTC as the session zone."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def _try_cast(column: str, spark_type: str):
    # try_cast turns unparseable values into nulls instead of failing the job,
    # so the data-quality step can count them. Integers go through double first ("1.0" -> 1).
    if spark_type == "int":
        return F.expr(f"try_cast(try_cast(`{column}` AS double) AS int)").alias(column)
    return F.expr(f"try_cast(`{column}` AS {spark_type})").alias(column)


def read_trips(spark: SparkSession, path: str) -> DataFrame:
    """Read a trips file (.parquet, .csv or .csv.gz) and return the columns in TRIP_COLUMNS."""
    if path.endswith(".parquet"):
        df = spark.read.parquet(path)
    else:
        df = spark.read.option("header", True).csv(path)

    missing = [c for c in TRIP_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"{path} is missing expected columns: {', '.join(missing)}")
    return df.select([_try_cast(c, t) for c, t in TRIP_COLUMNS.items()])


def read_zones(spark: SparkSession, path: str) -> DataFrame:
    """Read the taxi zone lookup (LocationID, Borough, Zone, service_zone)."""
    return (
        spark.read.option("header", True).csv(path)
        .select(
            F.col("LocationID").cast("int").alias("LocationID"),
            "Borough",
            "Zone",
            "service_zone",
        )
    )

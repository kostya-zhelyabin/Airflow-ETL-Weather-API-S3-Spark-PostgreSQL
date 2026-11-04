import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, timestamp_seconds


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--jdbc-url", required=True)
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-password", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("weather-transform-load").getOrCreate()
    try:
        raw = spark.read.option("multiline", "true").json(args.input)
        result = raw.select(
            timestamp_seconds(col("current.last_updated_epoch")).alias("observed_at"),
            col("location.lat").cast("double").alias("latitude"),
            col("location.lon").cast("double").alias("longitude"),
            col("current.temp_c").cast("double").alias("temperature_c"),
            col("current.humidity").cast("double").alias("relative_humidity_pct"),
            col("current.wind_kph").cast("double").alias("wind_speed_kmh"),
            lit(args.input).alias("source_file"),
            current_timestamp().alias("loaded_at"),
        )

        # One API response produces one row. Overwrite through JDBC is unsafe, so
        # duplicates are removed before append; the PK protects the final table.
        observed_at = result.first()["observed_at"]
        jdbc_options = {
            "url": args.jdbc_url,
            "user": args.db_user,
            "password": args.db_password,
            "driver": "org.postgresql.Driver",
        }
        existing = (
            spark.read.format("jdbc")
            .options(**jdbc_options)
            .option("query", f"SELECT observed_at FROM weather_observations WHERE observed_at = TIMESTAMPTZ '{observed_at}'")
            .load()
        )
        if existing.limit(1).count() == 0:
            result.write.format("jdbc").options(**jdbc_options).option(
                "dbtable", "weather_observations"
            ).mode("append").save()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()


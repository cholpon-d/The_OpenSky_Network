from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

def main():
    spark = SparkSession.builder \
            .appName("OpenSkyDataETL") \
            .config("spark.log.level", "WARN") \
            .getOrCreate()
            
    print("Starting Spark processing")

    try:

        df_raw = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/airflow") \
            .option("dbtable", "raw_flights") \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        print(f"Loaded {df_raw.count()} records from Postgres")


        if "geo_altitude" in df_raw.columns:
            df_raw = df_raw.withColumn("geo_altitude",
                when(col("geo_altitude").isNull(), col("baro_altitude"))
                .otherwise(col("geo_altitude")))

        df = (df_raw
            .filter(col("longitude").isNotNull() & col("latitude").isNotNull())
            .withColumn("event_time", from_unixtime(col("time_position")))
            .withColumn("event_date", to_date(col("event_time")))
            .withColumn("event_hour", hour(col("event_time")))
            .withColumn("event_weekday", dayofweek(col("event_time")))
            .withColumn("callsign", when(col("callsign").isNull(), "Unknown").otherwise(trim(col("callsign"))))
            .withColumn("callsign_length", length(col("callsign")))
            .withColumn("lat_sector", floor(col("latitude")))
            .withColumn("lon_sector", floor(col("longitude")))
            .withColumn("velocity_kmh", round(col("velocity") * 3.6, 2))
            .withColumn("is_anomaly", when((col("velocity") > 1200) | (col("baro_altitude") < 0), 1).otherwise(0))
            .withColumn("time_of_day", 
                        when((col("event_hour") >= 0) & (col("event_hour") < 6), "night")
                        .when((col("event_hour") >= 6) & (col("event_hour") < 12), "morning")
                        .when((col("event_hour") >= 12) & (col("event_hour") < 18), "day")
                        .otherwise("evening"))
            .withColumn("flight_status", when(col("on_ground") == True, "grounded").otherwise("flying"))
            .withColumn("velocity_category", 
                        when(col("velocity") < 300, "slow")
                        .when((col("velocity") >= 300) & (col("velocity") < 800), "medium")
                        .otherwise("fast"))
        )


        window_by_country_with_desc_velocity = Window.partitionBy("origin_country").orderBy(desc("velocity_kmh"))
        window_by_country = Window.partitionBy("origin_country")
        window_global_velocity = Window.orderBy(desc("velocity_kmh"))


        enriched_df = (df
            .withColumn("rank_in_country", rank().over(window_by_country_with_desc_velocity))
            .withColumn("speed_rank_global", rank().over(window_global_velocity))
            .withColumn("avg_speed_in_country", round(avg("velocity_kmh").over(window_by_country), 2))
            .withColumn("max_speed_in_country", round(max("velocity_kmh").over(window_by_country), 2))
            .withColumn("min_speed_in_country", round(min("velocity_kmh").over(window_by_country), 2))
        )


        window_total = Window().partitionBy()

        country_stats = (enriched_df
            .groupBy("origin_country")
            .agg(
                count("*").alias("total_aircraft"),
                round(avg("velocity_kmh"), 2).alias("avg_speed_kmh"),
                count_distinct("callsign").alias("unique_flights"),
                sum("is_anomaly").alias("anomaly_count")
            )
            .withColumn("global_rank", rank().over(Window.orderBy(desc("total_aircraft"))))
            .withColumn("percentage_of_total", round(col("total_aircraft") / sum("total_aircraft").over(window_total) * 100, 2))
            .orderBy(desc("total_aircraft"))
        )

        print("Writing to Postgres...")
        
        enriched_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/airflow") \
            .option("dbtable", "enriched_flights") \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        print("enriched_flights saved to Postgres")

        country_stats.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/airflow") \
            .option("dbtable", "country_stats") \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        print("country_stats saved to Postgres")

        write_to_clickhouse(enriched_df, country_stats)

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

    spark.stop()

def write_to_clickhouse(enriched_df, country_stats):
    try:
        print("Starting Clickhouse export...")

        clickhouse_url = "jdbc:clickhouse://clickhouse:8123/airflow"

        enriched_for_ch = enriched_df.select(
            "icao24", "callsign", "origin_country", "event_time", "event_date",
            coalesce(col("velocity_kmh"), lit(0)).alias("velocity_kmh"),
            coalesce(col("baro_altitude"), lit(0)).alias("baro_altitude"),
            coalesce(col("geo_altitude"), lit(0)).alias("geo_altitude"),
            "rank_in_country", "speed_rank_global", "avg_speed_in_country", 
            "max_speed_in_country", "min_speed_in_country", "is_anomaly", 
            "time_of_day", "flight_status"
        )

        enriched_for_ch.write \
            .format("jdbc") \
            .option("url", clickhouse_url) \
            .option("dbtable", "enriched_flights") \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("batchsize", "10000") \
            .mode("append") \
            .save()
        print("enriched_flights saved to ClickHouse")

        country_stats_for_ch = country_stats.select(
            "origin_country", "total_aircraft", "avg_speed_kmh", 
            "unique_flights", "anomaly_count", "global_rank", 
            "percentage_of_total"
        )

        country_stats_for_ch.write \
            .format("jdbc") \
            .option("url", clickhouse_url) \
            .option("dbtable", "country_stats") \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("batchsize", "10000") \
            .mode("append") \
            .save()
        print("country_stats saved to ClickHouse")

        clickhouse_optimized = enriched_df.select(
            "icao24", "callsign", "origin_country", "event_time", "event_date",
            coalesce(col("velocity_kmh"), lit(0)).alias("velocity_kmh"),
            coalesce(col("baro_altitude"), lit(0)).alias("baro_altitude"),
            coalesce(col("geo_altitude"), lit(0)).alias("geo_altitude"),
            "rank_in_country", "speed_rank_global", "is_anomaly",
            "time_of_day", "flight_status"
        )

        clickhouse_optimized.write \
            .format("jdbc") \
            .option("url", clickhouse_url) \
            .option("dbtable", "flights_analytics") \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("batchsize", "10000") \
            .mode("append") \
            .save()
        print("flights_analytics saved to ClickHouse")

    except Exception as e:
        print(f"ClickHouse error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
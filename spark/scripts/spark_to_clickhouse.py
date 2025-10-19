from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

def main():
    spark = SparkSession.builder \
            .appName("OpenSkyPostgres_to_ClickHouse") \
            .config("spark.log.level", "WARN") \
            .getOrCreate()
            
    print("Starting Spark processing")

    try:

        df_raw = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres-db:5432/airflow") \
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


        enriched_for_ch = enriched_df.select(
            "icao24", "callsign", "origin_country", "event_time", "event_date",
            coalesce(round(col("velocity_kmh"), 2), lit(0)).alias("velocity_kmh"),
            coalesce(round(col("baro_altitude"), 2), lit(0)).alias("baro_altitude"),
            coalesce(round(col("geo_altitude"), 2), lit(0)).alias("geo_altitude"),
            "rank_in_country", "speed_rank_global", 
            round(col("avg_speed_in_country"), 2).alias("avg_speed_in_country"),
            round(col("max_speed_in_country"), 2).alias("max_speed_in_country"),
            round(col("min_speed_in_country"), 2).alias("min_speed_in_country"),
            "is_anomaly", "time_of_day", "flight_status"
        )

        clickhouse_analytics = (enriched_df
            .groupBy("origin_country", "event_date", "time_of_day", "flight_status", "velocity_category")
            .agg(
                count("*").alias("flight_count"),
                round(avg("velocity_kmh"), 2).alias("avg_velocity_kmh"),
                round(avg("baro_altitude"), 2).alias("avg_baro_altitude"),
                round(avg("geo_altitude"), 2).alias("avg_geo_altitude"),
                sum("is_anomaly").alias("anomaly_count"),
                count_distinct("icao24").alias("unique_aircraft"),
                round(min("velocity_kmh"), 2).alias("min_velocity_kmh"),
                round(max("velocity_kmh"), 2).alias("max_velocity_kmh"),
                round(percentile_approx("velocity_kmh", 0.5), 2).alias("median_velocity_kmh")
            )
            .withColumn("anomaly_percentage", round((col("anomaly_count") / col("flight_count")) * 100, 2))
            .withColumn("load_timestamp", current_timestamp())
        )


        clickhouse_analytics_for_ch = clickhouse_analytics.select(
            "origin_country", "event_date", "time_of_day", "flight_status", "velocity_category",
            "flight_count", "avg_velocity_kmh", "avg_baro_altitude", "avg_geo_altitude",
            "anomaly_count", "unique_aircraft", "min_velocity_kmh", "max_velocity_kmh",
            "median_velocity_kmh", "anomaly_percentage"
        )
        
        country_stats_for_ch = country_stats.select(
            "origin_country", "total_aircraft", 
            round(col("avg_speed_kmh"), 2).alias("avg_speed_kmh"),
            "unique_flights", "anomaly_count", "global_rank", 
            round(col("percentage_of_total"), 2).alias("percentage_of_total")
        )

        clickhouse_url = "jdbc:clickhouse://clickhouse-server:8123/airflow"

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

        clickhouse_analytics_for_ch.write \
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

        print("All data successfully written to ClickHouse")

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

    spark.stop()


if __name__ == "__main__":
    main()
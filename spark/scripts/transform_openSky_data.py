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
        #print(f"Loaded {df_raw.count()} records from Postgres")
        #print("Data Schema: ")
       # df_raw.printSchema()
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
                    .withColumn("callsign", when(col("callsign").isNull(),"Unknown").otherwise(trim((col("callsign")))))
                    .withColumn("callsign_length", length(col("callsign")))
                    .withColumn("lat_sector", floor(col("latitude")))
                    .withColumn("lon_sector", floor(col("longitude")))
                    .withColumn("velocity_kmh", round(col("velocity") * 3.6,2))   # конвертация в км/ч
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
        #df.show(5)

        window_by_country_with_desc_velocity = Window.partitionBy("origin_country").orderBy(desc("velocity_kmh"))
        window_by_country = Window.partitionBy("origin_country")
        window_global_velocity = Window.orderBy(desc("velocity_kmh"))

        enriched_df = (df
                        .withColumn("rank_in_country", rank().over(window_by_country_with_desc_velocity))
                        .withColumn("speed_rank_global", rank().over(window_global_velocity))
                        .withColumn("avg_speed_in_country", avg("velocity_kmh").over(window_by_country))
                        .withColumn("max_speed_in_country", max("velocity_kmh").over(window_by_country))
                        .withColumn("min_speed_in_country", min("velocity_kmh").over(window_by_country))
                        )
        #enriched_df.show(5)

        window_total = Window().partitionBy()

        country_stats = (enriched_df
                            .groupBy("origin_country")
                            .agg(count("*").alias("total_aircraft"),
                            round(avg("velocity_kmh"),2).alias("avg_speed_kmh"),
                            count_distinct("callsign").alias("unique_flights"))
                            .withColumn("global_rank", rank().over(Window.orderBy(desc("total_aircraft"))))
                            .withColumn("percentage_of_total",round(col("total_aircraft") / sum("total_aircraft").over(window_total) * 100,2))
                            .orderBy(desc("total_aircraft"))
                        )
        #country_stats.show(10,truncate=False)
        enriched_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/airflow") \
            .option("dbtable", "enriched_flights") \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        print(f"{enriched_df} saved to Postgres")

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

        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/airflow") \
            .option("dbtable", "processed_flights") \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        print("processed_flights saved to Postgres")
            

    except Exception as e:
        print(f"ERROR: {e}")

    spark.stop()

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
NYC Yellow Taxi ETL (Starter Template with TODOs)

Implement:
- Read monthly Parquet files and the taxi zone lookup CSV
- Clean and transform trip data (types, invalid values, derived columns)
- Join to enrich trips with zone names
- Aggregations:
  1) Hourly pickups per zone
  2) Top 10 zones by daily trip counts
  3) Weekly revenue per vendor
- Write curated Parquet partitioned by pickup_date
- (Optional) Write aggregations to Snowflake (key-pair auth supported via SF_PRIVATE_KEY_B64)
"""
import argparse
import os
import base64
from typing import List, Dict
from functools import reduce
import time
from pyspark.sql import SparkSession, DataFrame, functions as F, types as T

def build_spark(app_name: str, shuffle_partitions: int = 200) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .getOrCreate()
    )
    return spark

def read_trips(spark: SparkSession, input_paths: List[str]) -> DataFrame:
    # Read multiple Parquet files, e.g., spark.read.parquet(*input_paths)
    dfs = []
    for path in input_paths:
        df = spark.read.parquet(path)
        
        # Normalize schema differences before union
        # Airport_fee vs airport_fee
        columns_lower = {col.lower(): col for col in df.columns}
        if 'airport_fee' in columns_lower and columns_lower['airport_fee'] != 'airport_fee':
            df = df.withColumnRenamed(columns_lower['airport_fee'], 'airport_fee')

        # Cast fields to avoid schema conflicts
        # Handle Int32/Int64 and Double/Int64
        df = df.withColumn("VendorID", F.col("VendorID").cast(T.IntegerType())) \
               .withColumn("passenger_count", F.col("passenger_count").cast(T.IntegerType())) \
               .withColumn("RatecodeID", F.col("RatecodeID").cast(T.IntegerType())) \
               .withColumn("PULocationID", F.col("PULocationID").cast(T.IntegerType())) \
               .withColumn("DOLocationID", F.col("DOLocationID").cast(T.IntegerType()))
        
        dfs.append(df)
    
    # Union all data sources
    return reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dfs)

def read_zones(spark: SparkSession, zone_csv: str) -> DataFrame:
    # Read CSV with header=True, inferSchema=True
    return spark.read.csv(zone_csv, header=True, inferSchema=True)

def clean_and_transform(df: DataFrame) -> DataFrame:
    """
    Required:
    - Cast fields to correct types (timestamps, doubles/integers)
    - Filter invalid trips:
      * passenger_count <= 0
      * trip_distance <= 0
      * fare_amount < 0
    - Derive:
      * pickup_date (date)
      * pickup_hour (0-23)
      * week_of_year (1-53)
      * vendor_name (1->'Creative', 2->'VeriFone', else id string or 'Unknown')
    """
    
    # Cast remaining fields to correct types
    df_clean = df.withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast(T.TimestampType())) \
                 .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast(T.TimestampType())) \
                 .withColumn("trip_distance", F.col("trip_distance").cast(T.DoubleType())) \
                 .withColumn("fare_amount", F.col("fare_amount").cast(T.DoubleType())) \
                 .withColumn("total_amount", F.col("total_amount").cast(T.DoubleType()))
    
    # Filter invalid trips
    df_filtered = df_clean.filter(
        (F.col("passenger_count") > 0) &
        (F.col("trip_distance") > 0) &
        (F.col("fare_amount") >= 0)
    )
    
    # Derive additional columns
    df_transformed = df_filtered \
        .withColumn("pickup_date", F.to_date(F.col("tpep_pickup_datetime"))) \
        .withColumn("pickup_hour", F.hour(F.col("tpep_pickup_datetime"))) \
        .withColumn("week_of_year", F.weekofyear(F.col("tpep_pickup_datetime"))) \
        .withColumn("vendor_name", 
            F.when(F.col("VendorID") == 1, "Creative")
             .when(F.col("VendorID") == 2, "VeriFone")
             .otherwise("Unknown")
        )
    
    return df_transformed

def join_zones(df: DataFrame, zones: DataFrame) -> DataFrame:
    """
    Join PULocationID and DOLocationID to zone names.
    - zones: LocationID, Borough, Zone
    - Consider broadcasting zones (small table)
    """
    zones_broadcast = F.broadcast(zones)
    
    # Join pickup location
    df = df.join(
        zones_broadcast,
        df.PULocationID == zones_broadcast.LocationID,
        "left"
    ).withColumnRenamed("Borough", "PU_Borough") \
      .withColumnRenamed("Zone", "PU_Zone") \
      .withColumnRenamed("service_zone", "PU_service_zone") \
      .drop("LocationID")
    
    # Join dropoff location
    df = df.join(
        zones_broadcast,
        df.DOLocationID == zones_broadcast.LocationID,
        "left"
    ).withColumnRenamed("Borough", "DO_Borough") \
      .withColumnRenamed("Zone", "DO_Zone") \
      .withColumnRenamed("service_zone", "DO_service_zone") \
      .drop("LocationID")

    return df

def agg_hourly_pickups(df_enriched: DataFrame) -> DataFrame:
    # Group by pickup_date, pickup_hour, PU_Zone (or PU_Borough) and count
    return df_enriched.groupBy("pickup_date", "pickup_hour", "PU_Zone").count()

def agg_top10_zones_daily(df_enriched: DataFrame) -> DataFrame:
    # Daily top 10 zones by trips with rank
    return df_enriched.groupBy("pickup_date", "PU_Zone").count().orderBy("count", ascending=False).limit(10)

def agg_weekly_revenue_per_vendor(df_enriched: DataFrame) -> DataFrame:
    # Sum total_amount by vendor_name and week_of_year
    return df_enriched.groupBy("vendor_name", "week_of_year").sum("total_amount")

def write_curated(df_enriched: DataFrame, out_path: str):
    # Write partitioned by pickup_date as parquet (mode overwrite or append)
    df_enriched.write \
        .partitionBy("pickup_date") \
        .mode("overwrite") \
        .parquet(out_path)

def write_aggregates_local(dfs: Dict[str, DataFrame], out_root: str):
    # Write each df as parquet under out_root/<name>
    for name, df in dfs.items():
        output_path = os.path.join(out_root, name)
        df.write.mode("overwrite").parquet(output_path)

def _decode_pem_from_b64(b64_str: str) -> str:
    if not b64_str:
        return ""
    return base64.b64decode(b64_str).decode("utf-8")

def write_to_snowflake(dfs: Dict[str, DataFrame], sf_opts: Dict[str, str], mode: str = "overwrite"):
    """
    Requires Spark Snowflake connector on classpath.

    For key-pair auth, provide:
      - sfURL, sfUser, sfDatabase, sfSchema, sfWarehouse
      - pem_private_key (decoded PEM string) and optional pem_private_key_passphrase

    Hints (TODO for students):
    - Construct writer = df.write.format("snowflake").options(**opts).option("dbtable", "<TABLE>")
    - Call writer.mode(mode).save()
    """
    # Implement Snowflake writes using sf_opts.
    for table_name, df in dfs.items():
        print(f"Writing {table_name} to Snowflake...")
        
        # Build Snowflake options
        writer = df.write.format("snowflake").options(**sf_opts)
        
        # Set the table name
        writer = writer.option("dbtable", table_name)
        
        # Write with specified mode
        writer.mode(mode).save()
        
        print(f"Successfully wrote {table_name} to Snowflake")

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input_paths", nargs="+", required=True)
    p.add_argument("--zone_csv", required=True)
    p.add_argument("--curated_out", required=True)
    p.add_argument("--aggregates_out", required=True)
    p.add_argument("--shuffle_partitions", type=int, default=200)
    # Snowflake options
    p.add_argument("--write_snowflake", type=str, default="false")
    p.add_argument("--sfURL", default=os.getenv("SF_URL", ""))
    p.add_argument("--sfUser", default=os.getenv("SF_USER", ""))
    p.add_argument("--sfPassword", default=os.getenv("SF_PASSWORD", ""))  # optional fallback
    p.add_argument("--sfDatabase", default=os.getenv("SF_DATABASE", ""))
    p.add_argument("--sfSchema", default=os.getenv("SF_SCHEMA", ""))
    p.add_argument("--sfWarehouse", default=os.getenv("SF_WAREHOUSE", ""))
    p.add_argument("--sfAuthenticator", default=os.getenv("SF_AUTHENTICATOR", "snowflake"))
    p.add_argument("--sfPrivateKeyB64", default=os.getenv("SF_PRIVATE_KEY_B64", ""))
    p.add_argument("--sfPrivateKeyPassphrase", default=os.getenv("SF_PRIVATE_KEY_PASSPHRASE", ""))
    return p.parse_args()

def main():
    args = parse_args()
    spark = build_spark("NYC Taxi ETL (Student)", shuffle_partitions=args.shuffle_partitions)

    trips = read_trips(spark, args.input_paths)
    zones = read_zones(spark, args.zone_csv)
    clean = clean_and_transform(trips)
    enriched = join_zones(clean, zones)

    # Aggregations
    hourly = agg_hourly_pickups(enriched)
    top10 = agg_top10_zones_daily(enriched)
    weekly_rev = agg_weekly_revenue_per_vendor(enriched)

    # Outputs
    write_curated(enriched, args.curated_out)
    write_aggregates_local(
        {"hourly_pickups": hourly, "top10_zones_daily": top10, "weekly_revenue_vendor": weekly_rev},
        args.aggregates_out,
    )

    if args.write_snowflake.lower() == "true":
        sf_opts = {
            "sfURL": args.sfURL,
            "sfUser": args.sfUser,
            "sfDatabase": args.sfDatabase,
            "sfSchema": args.sfSchema,
            "sfWarehouse": args.sfWarehouse,
            "sfAuthenticator": args.sfAuthenticator,
        }
        # Prefer key-pair if provided
        pem = _decode_pem_from_b64(args.sfPrivateKeyB64)
        if pem:
            sf_opts["pem_private_key"] = pem
            if args.sfPrivateKeyPassphrase:
                sf_opts["pem_private_key_passphrase"] = args.sfPrivateKeyPassphrase
        elif args.sfPassword:
            # Fallback to username/password if allowed by your account policy
            sf_opts["sfPassword"] = args.sfPassword

        write_to_snowflake(
            {"HOURLY_PICKUPS": hourly, "TOP10_ZONES_DAILY": top10, "WEEKLY_REVENUE_VENDOR": weekly_rev},
            sf_opts,
            mode="overwrite",
        )

    # Keep Spark UI running for 10 minutes after job completes
    print("Spark UI: http://localhost:4041")
    time.sleep(600)
    
    spark.stop()

if __name__ == "__main__":
    main()

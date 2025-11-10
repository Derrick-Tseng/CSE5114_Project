from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, regexp_extract, input_file_name, 
    broadcast, coalesce, when, min as spark_min, max as spark_max
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
import os


def create_spark_session():
    spark = SparkSession.builder \
        .appName("GDP-Bulk-Load-Bronze") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.sql.warehouse.dir", "/opt/spark/data/bronze") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def define_csv_schema():
    return StructType([
        StructField("observation_date", StringType(), True),
        StructField("gdp_value", IntegerType(), True)
    ])


def load_fips_mapping(spark, base_path):
    mapping_path = f"file://{os.path.abspath(base_path)}/fips_mapping.csv"
    
    schema = StructType([
        StructField("fips_code", StringType(), True),
        StructField("state_code", StringType(), True),
        StructField("state_name", StringType(), True),
        StructField("county_name", StringType(), True),
        StructField("county_code", StringType(), True)
    ])
    
    mapping_df = spark.read.format("csv") \
        .option("header", "true") \
        .schema(schema) \
        .load(mapping_path)
    
    return mapping_df


def bulk_read_csv_files(spark, gdp_data_path, csv_schema):
    gdp_path_pattern = f"file://{os.path.abspath(gdp_data_path)}/gdpall*.csv"
    
    df_raw = spark.read.format("csv") \
        .option("header", "true") \
        .schema(csv_schema) \
        .load(gdp_path_pattern)
    
    return df_raw


def extract_fips_from_filename(df_raw):
    df_with_fips = df_raw.withColumn(
        "fips_code",
        regexp_extract(input_file_name(), r"gdpall(\d{5})\.csv", 1)
    )
    
    df_with_fips = df_with_fips.filter(
        (col("fips_code") != "") & (col("fips_code") != "02261")
    )
    
    return df_with_fips


def enrich_with_fips_mapping(df_with_fips, fips_mapping):
    df_enriched = df_with_fips.join(
        broadcast(fips_mapping),
        on="fips_code",
        how="left"
    )
    
    df_bronze = df_enriched.select(
        col("fips_code"),
        col("state_code"),
        col("state_name"),
        col("county_code"),
        col("county_name"),
        col("observation_date").alias("date"),
        col("gdp_value")
    )
    
    null_state_count = df_bronze.filter(col("state_code").isNull()).count()
    
    if null_state_count > 0:
        print(f"Warning: {null_state_count} records with unmapped FIPS codes")
    
    return df_bronze


def optimize_partitioning(df_bronze, num_partitions=200):
    df_repartitioned = df_bronze.repartition(num_partitions, "state_code")
    return df_repartitioned


def write_bronze_table(df_repartitioned, output_path):
    """Write Bronze table partitioned by state_code."""
    bronze_path = f"file://{os.path.abspath(output_path)}/bronze/gdp_data"
    
    df_repartitioned.write \
        .mode("overwrite") \
        .format("parquet") \
        .partitionBy("state_code") \
        .option("compression", "snappy") \
        .save(bronze_path)
    
    return bronze_path


def main():
    spark = create_spark_session()
    
    base_path = "/opt/spark/data"
    gdp_data_path = f"{base_path}/gdp"
    output_path = base_path
    
    try:
        csv_schema = define_csv_schema()
        fips_mapping = load_fips_mapping(spark, base_path)
        df_raw = bulk_read_csv_files(spark, gdp_data_path, csv_schema)
        df_with_fips = extract_fips_from_filename(df_raw)
        df_bronze = enrich_with_fips_mapping(df_with_fips, fips_mapping)
        df_repartitioned = optimize_partitioning(df_bronze, num_partitions=200)
        bronze_path = write_bronze_table(df_repartitioned, output_path)
        
        print(f"Bronze table created: {bronze_path}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

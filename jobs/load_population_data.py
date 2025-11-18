from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lpad, substring, broadcast
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os


def create_spark_session():
    spark = SparkSession.builder \
        .appName("Population-Data-Load") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.sql.warehouse.dir", "/opt/spark/data/bronze") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def define_population_schema():
    return StructType([
        StructField("IBRC_Geo_ID", StringType(), True),
        StructField("Statefips", StringType(), True),
        StructField("Countyfips", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Year", IntegerType(), True),
        StructField("Total_Population", IntegerType(), True),
        StructField("Population_0_4", IntegerType(), True),
        StructField("Population_5_17", IntegerType(), True),
        StructField("Population_18_24", IntegerType(), True),
        StructField("Population_25_44", IntegerType(), True),
        StructField("Population_45_64", IntegerType(), True),
        StructField("Population_65_plus", IntegerType(), True),
        StructField("Population_Under_18", IntegerType(), True),
        StructField("Population_18_54", IntegerType(), True),
        StructField("Population_55_plus", IntegerType(), True),
        StructField("Male_Population", IntegerType(), True),
        StructField("Female_Population", IntegerType(), True)
    ])


def load_population_data(spark, population_csv_path):
    if not os.path.exists(population_csv_path):
        raise ValueError(f"Population CSV not found: {population_csv_path}")
    
    schema = define_population_schema()
    
    df = spark.read.format("csv") \
        .option("header", "true") \
        .schema(schema) \
        .load(f"file://{os.path.abspath(population_csv_path)}")
    
    df = df.filter(col("IBRC_Geo_ID") != "0")
    
    df = df.withColumn(
        "fips_code",
        lpad(col("IBRC_Geo_ID"), 5, "0")
    )
    
    df = df.withColumn(
        "is_state_level",
        when(substring(col("fips_code"), 3, 3) == "000", True).otherwise(False)
    )
    
    df = df.withColumn(
        "state_code",
        substring(col("fips_code"), 1, 2)
    )
    
    df = df.withColumn(
        "county_code",
        substring(col("fips_code"), 3, 3)
    )
    
    return df


def create_county_population_bronze(df_population):
    df_county = df_population.filter(col("is_state_level") == False)
    
    df_county_bronze = df_county.select(
        col("fips_code"),
        col("state_code"),
        col("county_code"),
        col("Description").alias("location_name"),
        col("Year").alias("year"),
        col("Total_Population").alias("total_population"),
        col("Population_0_4").alias("pop_0_4"),
        col("Population_5_17").alias("pop_5_17"),
        col("Population_18_24").alias("pop_18_24"),
        col("Population_25_44").alias("pop_25_44"),
        col("Population_45_64").alias("pop_45_64"),
        col("Population_65_plus").alias("pop_65_plus"),
        col("Population_Under_18").alias("pop_under_18"),
        col("Population_18_54").alias("pop_18_54"),
        col("Population_55_plus").alias("pop_55_plus"),
        col("Male_Population").alias("male_population"),
        col("Female_Population").alias("female_population")
    )
    
    return df_county_bronze


def create_state_population_bronze(df_population):
    df_state = df_population.filter(col("is_state_level") == True)
    
    df_state_bronze = df_state.select(
        col("state_code"),
        col("Description").alias("state_name"),
        col("Year").alias("year"),
        col("Total_Population").alias("total_population"),
        col("Population_0_4").alias("pop_0_4"),
        col("Population_5_17").alias("pop_5_17"),
        col("Population_18_24").alias("pop_18_24"),
        col("Population_25_44").alias("pop_25_44"),
        col("Population_45_64").alias("pop_45_64"),
        col("Population_65_plus").alias("pop_65_plus"),
        col("Population_Under_18").alias("pop_under_18"),
        col("Population_18_54").alias("pop_18_54"),
        col("Population_55_plus").alias("pop_55_plus"),
        col("Male_Population").alias("male_population"),
        col("Female_Population").alias("female_population")
    )
    
    return df_state_bronze


def write_bronze_table(df, output_path, table_name, partition_col=None):
    bronze_path = f"file://{os.path.abspath(output_path)}/bronze/{table_name}"
    
    writer = df.write.mode("overwrite").format("parquet").option("compression", "snappy")
    
    if partition_col:
        writer = writer.partitionBy(partition_col)
    
    writer.save(bronze_path)
    
    return bronze_path


def validate_bronze_table(spark, bronze_path, table_name):
    df = spark.read.format("parquet").load(bronze_path)
    
    total_records = df.count()
    num_years = df.select("year").distinct().count()
    
    if table_name == "population_county":
        num_counties = df.select("fips_code").distinct().count()
        print(f"{table_name}: {total_records:,} records, {num_counties} counties, {num_years} years")
    else:
        num_states = df.select("state_code").distinct().count()
        print(f"{table_name}: {total_records:,} records, {num_states} states, {num_years} years")


def main():
    spark = create_spark_session()
    
    base_path = "/opt/spark/data"
    population_csv = f"{base_path}/population/Population by Age and Sex - US, States, Counties.csv"
    output_path = base_path
    
    try:
        df_population = load_population_data(spark, population_csv)
        
        df_county = create_county_population_bronze(df_population)
        county_path = write_bronze_table(df_county, output_path, "population_county", "state_code")
        validate_bronze_table(spark, county_path, "population_county")
        
        df_state = create_state_population_bronze(df_population)
        state_path = write_bronze_table(df_state, output_path, "population_state", "state_code")
        validate_bronze_table(spark, state_path, "population_state")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, first, last, count, countDistinct, avg, struct, sum as spark_sum,
    min as spark_min, max as spark_max, lit, lag, round as spark_round, row_number
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
import os
import glob
import re

def create_spark_session():
    spark = SparkSession.builder \
        .appName("GDP-Data-Processing") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.sql.warehouse.dir", "/opt/spark/data/processed") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def process_gdp_data(spark, bronze_data_path):
    """Read and validate Bronze table."""
    if not os.path.exists(bronze_data_path):
        raise ValueError(f"Bronze data path does not exist: {bronze_data_path}")
    
    try:
        gdp_df = spark.read.format("parquet").load(f"file://{os.path.abspath(bronze_data_path)}")
        
        record_count = gdp_df.count()
        if record_count == 0:
            raise ValueError("Bronze table is empty")
        
        gdp_df = gdp_df.withColumn("date", col("date").cast("date")) \
                       .withColumn("year", year(col("date")))
        
        gdp_df = gdp_df.filter(
            col("fips_code").isNotNull() & 
            col("date").isNotNull() & 
            col("gdp_value").isNotNull() &
            (col("fips_code") != "02261")
        )
        
        return gdp_df
        
    except Exception as e:
        print(f"Error reading Bronze table: {e}")
        raise

def calculate_gdp_statistics(gdp_df):
    """Calculate GDP statistics and growth rates."""
    # Base table with GDP data ordered by location and year
    gdp_by_year = gdp_df.select(
        "state_code", "state_name", "fips_code", "county_name", "year", "date", "gdp_value"
    ).orderBy("state_code", "fips_code", "year")
    
    # Calculate year-over-year growth rates per county
    window_spec = Window.partitionBy("fips_code").orderBy("year")
    
    gdp_with_growth = gdp_df.withColumn(
        "prev_year_gdp", 
        lag("gdp_value").over(window_spec)
    ).withColumn(
        "yoy_growth_rate",
        spark_round(
            ((col("gdp_value") - col("prev_year_gdp")) / col("prev_year_gdp") * 100),
            2
        )
    ).select(
        "state_code",
        "state_name",
        "fips_code",
        "county_name",
        "year", 
        "gdp_value",
        "prev_year_gdp",
        "yoy_growth_rate"
    ).orderBy("state_code", "fips_code", "year")
    
    # Find maximum YoY growth rate and year for each county
    max_yoy_data = gdp_with_growth.filter(col("yoy_growth_rate").isNotNull()) \
        .groupBy("state_code", "state_name", "fips_code", "county_name") \
        .agg(
            spark_max(struct(col("yoy_growth_rate"), col("year"))).alias("max_yoy_struct")
        ) \
        .select(
            col("state_code"),
            col("state_name"),
            col("fips_code"),
            col("county_name"),
            col("max_yoy_struct.yoy_growth_rate").alias("max_yoy"),
            col("max_yoy_struct.year").alias("max_yoy_year")
        )
    
    # Find minimum YoY growth rate and year for each county
    min_yoy_data = gdp_with_growth.filter(col("yoy_growth_rate").isNotNull()) \
        .groupBy("state_code", "state_name", "fips_code", "county_name") \
        .agg(
            spark_min(struct(col("yoy_growth_rate"), col("year"))).alias("min_yoy_struct")
        ) \
        .select(
            col("state_code"),
            col("state_name"),
            col("fips_code"),
            col("county_name"),
            col("min_yoy_struct.yoy_growth_rate").alias("min_yoy"),
            col("min_yoy_struct.year").alias("min_yoy_year")
        )
    
    # Calculate average YoY growth rate for each county
    avg_yoy_data = gdp_with_growth.filter(col("yoy_growth_rate").isNotNull()) \
        .groupBy("state_code", "state_name", "fips_code", "county_name") \
        .agg(
            spark_round(avg("yoy_growth_rate"), 2).alias("average_yoy")
        )
    
    # Calculate maximum drawdown (largest decline from peak GDP)
    peak_window = Window.partitionBy("fips_code").orderBy("year").rowsBetween(Window.unboundedPreceding, 0)
    
    drawdown_df = gdp_df.withColumn(
        "peak_gdp",
        spark_max("gdp_value").over(peak_window)
    ).withColumn(
        "drawdown_pct",
        spark_round(
            ((col("gdp_value") - col("peak_gdp")) / col("peak_gdp") * 100),
            2
        )
    )
    
    max_drawdown_data = drawdown_df.filter(col("drawdown_pct") < 0) \
        .groupBy("state_code", "state_name", "fips_code", "county_name") \
        .agg(
            spark_min(struct(col("drawdown_pct"), col("year"))).alias("max_dd_struct")
        ) \
        .select(
            col("state_code"),
            col("state_name"),
            col("fips_code"),
            col("county_name"),
            col("max_dd_struct.drawdown_pct").alias("maximum_draw_down"),
            col("max_dd_struct.year").alias("maximum_draw_down_year")
        )
    
    # Aggregate county-level summary with all growth metrics
    county_summary = gdp_df.groupBy("state_code", "state_name", "fips_code", "county_name").agg(
        spark_min("year").alias("first_year"),
        spark_max("year").alias("last_year"),
        count("*").alias("years_of_data")
    )
    
    county_summary = county_summary \
        .join(avg_yoy_data, ["state_code", "state_name", "fips_code", "county_name"], "left") \
        .join(max_yoy_data, ["state_code", "state_name", "fips_code", "county_name"], "left") \
        .join(min_yoy_data, ["state_code", "state_name", "fips_code", "county_name"], "left") \
        .join(max_drawdown_data, ["state_code", "state_name", "fips_code", "county_name"], "left") \
        .select(
            "state_code",
            "state_name",
            "fips_code",
            "county_name",
            "first_year",
            "last_year",
            "years_of_data",
            "average_yoy",
            "max_yoy",
            "max_yoy_year",
            "min_yoy",
            "min_yoy_year",
            "maximum_draw_down",
            "maximum_draw_down_year"
        ) \
        .orderBy("state_code", "fips_code")
    
    # State-level summary with basic county counts and date range
    state_summary = gdp_df.groupBy("state_code", "state_name").agg(
        countDistinct("fips_code").alias("num_counties"),
        spark_min("year").alias("first_year"),
        spark_max("year").alias("last_year")
    )
    
    # State-level growth statistics aggregated from counties
    state_growth = gdp_with_growth.filter(col("yoy_growth_rate").isNotNull()) \
        .groupBy("state_code", "state_name") \
        .agg(
            spark_round(avg("yoy_growth_rate"), 2).alias("avg_county_yoy"),
            spark_max("yoy_growth_rate").alias("max_county_yoy"),
            spark_min("yoy_growth_rate").alias("min_county_yoy")
        )
    
    # Calculate average growth per county for ranking
    county_avg_growth = gdp_with_growth.filter(col("yoy_growth_rate").isNotNull()) \
        .groupBy("state_code", "state_name", "fips_code", "county_name") \
        .agg(spark_round(avg("yoy_growth_rate"), 2).alias("avg_growth"))
    
    # Identify best performing county per state by average growth
    window_best = Window.partitionBy("state_code").orderBy(col("avg_growth").desc())
    window_worst = Window.partitionBy("state_code").orderBy(col("avg_growth").asc())
    
    best_counties = county_avg_growth.withColumn("rank", row_number().over(window_best)) \
        .filter(col("rank") == 1) \
        .select(
            col("state_code"),
            col("county_name").alias("best_perform_county"),
            col("avg_growth").alias("best_perform_avg_yoy")
        )
    
    # Identify worst performing county per state by average growth
    worst_counties = county_avg_growth.withColumn("rank", row_number().over(window_worst)) \
        .filter(col("rank") == 1) \
        .select(
            col("state_code"),
            col("county_name").alias("least_perform_county"),
            col("avg_growth").alias("least_perform_avg_yoy")
        )
    
    state_summary = state_summary \
        .join(state_growth, ["state_code", "state_name"], "left") \
        .join(best_counties, ["state_code"], "left") \
        .join(worst_counties, ["state_code"], "left") \
        .orderBy("state_code")
    
    # State-level GDP totals by year
    state_year_gdp = gdp_df.groupBy("state_code", "state_name", "year").agg(
        countDistinct("fips_code").alias("number_of_county"),
        spark_sum("gdp_value").alias("total_gdp")
    )
    
    county_year_gdp = gdp_df.select(
        "state_code", "state_name", "fips_code", "county_name", "year", "gdp_value"
    )
    
    # Identify best and worst performing counties per state per year by GDP value
    window_best_year = Window.partitionBy("state_code", "year").orderBy(col("gdp_value").desc())
    window_worst_year = Window.partitionBy("state_code", "year").orderBy(col("gdp_value").asc())
    
    best_counties_year = county_year_gdp.withColumn("rank", row_number().over(window_best_year)) \
        .filter(col("rank") == 1) \
        .select(
            col("state_code"),
            col("year"),
            col("county_name").alias("best_perform_county"),
            col("gdp_value").alias("best_perform_county_gdp")
        )
    
    worst_counties_year = county_year_gdp.withColumn("rank", row_number().over(window_worst_year)) \
        .filter(col("rank") == 1) \
        .select(
            col("state_code"),
            col("year"),
            col("county_name").alias("least_perform_county"),
            col("gdp_value").alias("least_perform_county_gdp")
        )
    
    gdp_by_state_by_year = state_year_gdp \
        .join(best_counties_year, ["state_code", "year"], "left") \
        .join(worst_counties_year, ["state_code", "year"], "left") \
        .select(
            "state_name",
            "state_code",
            "year",
            "number_of_county",
            "total_gdp",
            "best_perform_county",
            "best_perform_county_gdp",
            "least_perform_county",
            "least_perform_county_gdp"
        ) \
        .orderBy("state_code", "year")
    
    return gdp_by_year, gdp_with_growth, county_summary, state_summary, gdp_by_state_by_year

def save_to_parquet(df, output_path):
    """Save data to Parquet format."""
    import shutil
    
    parent_dir = os.path.dirname(output_path)
    if not os.path.exists(parent_dir):
        os.makedirs(parent_dir, exist_ok=True)
    
    if os.path.exists(output_path):
        shutil.rmtree(output_path)
    
    df.write.mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .save(output_path)

def main():
    spark = create_spark_session()
    
    base_path = "/opt/spark/data"
    bronze_path = f"{base_path}/bronze/gdp_data"
    gold_path = f"{base_path}/gold"
    
    try:
        if not os.path.exists(bronze_path):
            raise ValueError(f"Bronze table not found: {bronze_path}")
        
        gdp_df = process_gdp_data(spark, bronze_path)
        
        gdp_by_year, gdp_with_growth, county_summary, state_summary, gdp_by_state_by_year = calculate_gdp_statistics(gdp_df)
        
        save_to_parquet(gdp_by_year, f"{gold_path}/gdp_by_year")
        save_to_parquet(gdp_with_growth, f"{gold_path}/gdp_with_growth")
        save_to_parquet(county_summary, f"{gold_path}/gdp_county_summary")
        save_to_parquet(state_summary, f"{gold_path}/gdp_state_summary")
        save_to_parquet(gdp_by_state_by_year, f"{gold_path}/gdp_by_state_by_year")

        print(f"Parquet files saved to: {gold_path}")
        
    except Exception as e:
        print(f"Error during processing: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

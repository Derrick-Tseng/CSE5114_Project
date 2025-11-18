from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, first, last, count, countDistinct, avg, struct, sum as spark_sum,
    min as spark_min, max as spark_max, lit, lag, round as spark_round, row_number, broadcast
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
import os
import glob
import re


def create_spark_session():
    spark = SparkSession.builder \
        .appName("Population-Data-Processing") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.sql.warehouse.dir", "/opt/spark/data/processed") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_fips_mapping(spark, mapping_path):
    """Load FIPS mapping to get state and county names."""
    if not os.path.exists(mapping_path):
        raise ValueError(f"FIPS mapping file not found: {mapping_path}")
    
    fips_df = spark.read.format("csv") \
        .option("header", "true") \
        .load(f"file://{os.path.abspath(mapping_path)}")
    
    # Select relevant columns and rename
    fips_df = fips_df.select(
        col("FIPS_Code").alias("fips_code"),
        col("State_Name").alias("state_name"),
        col("County_Name").alias("county_name")
    )
    
    return fips_df


def process_population_data(spark, bronze_data_path, fips_mapping_path):
    """Read and validate Bronze table, and enrich with location names."""
    if not os.path.exists(bronze_data_path):
        raise ValueError(f"Bronze data path does not exist: {bronze_data_path}")
    
    try:
        pop_df = spark.read.format("parquet").load(f"file://{os.path.abspath(bronze_data_path)}")
        
        record_count = pop_df.count()
        if record_count == 0:
            raise ValueError("Bronze table is empty")
        
        # Filter out null values
        pop_df = pop_df.filter(
            col("fips_code").isNotNull() & 
            col("year").isNotNull() & 
            col("total_population").isNotNull()
        )
        
        # Load FIPS mapping to get state and county names
        fips_df = load_fips_mapping(spark, fips_mapping_path)
        
        # Join with FIPS mapping to add state_name and county_name
        pop_df = pop_df.join(
            broadcast(fips_df),
            on="fips_code",
            how="left"
        )
        
        return pop_df
        
    except Exception as e:
        print(f"Error reading Bronze table: {e}")
        raise


def calculate_population_statistics(pop_df):
    """Calculate population statistics and growth rates."""
    
    # Base table with population data ordered by location and year
    pop_by_year = pop_df.select(
        "state_code", "state_name", "fips_code", "county_name", "year", 
        "total_population", "male_population", "female_population",
        "pop_0_4", "pop_5_17", "pop_18_24", "pop_25_44", "pop_45_64", "pop_65_plus"
    ).orderBy("state_code", "fips_code", "year")
    
    # Calculate year-over-year growth rates per county
    window_spec = Window.partitionBy("fips_code").orderBy("year")
    
    pop_with_growth = pop_df.withColumn(
        "prev_year_population", 
        lag("total_population").over(window_spec)
    ).withColumn(
        "yoy_growth_rate",
        spark_round(
            ((col("total_population") - col("prev_year_population")) / col("prev_year_population") * 100),
            2
        )
    ).withColumn(
        "yoy_growth_absolute",
        (col("total_population") - col("prev_year_population")).cast("integer")
    )
    
    # Calculate demographic percentages
    pop_with_growth = pop_with_growth.withColumn(
        "pct_0_4",
        spark_round((col("pop_0_4") / col("total_population") * 100), 2)
    ).withColumn(
        "pct_5_17",
        spark_round((col("pop_5_17") / col("total_population") * 100), 2)
    ).withColumn(
        "pct_18_24",
        spark_round((col("pop_18_24") / col("total_population") * 100), 2)
    ).withColumn(
        "pct_25_44",
        spark_round((col("pop_25_44") / col("total_population") * 100), 2)
    ).withColumn(
        "pct_45_64",
        spark_round((col("pop_45_64") / col("total_population") * 100), 2)
    ).withColumn(
        "pct_65_plus",
        spark_round((col("pop_65_plus") / col("total_population") * 100), 2)
    ).withColumn(
        "pct_male",
        spark_round((col("male_population") / col("total_population") * 100), 2)
    ).withColumn(
        "pct_female",
        spark_round((col("female_population") / col("total_population") * 100), 2)
    ).withColumn(
        "working_age_population",
        col("pop_18_24") + col("pop_25_44") + col("pop_45_64")
    ).withColumn(
        "dependency_ratio",
        spark_round(
            ((col("pop_0_4") + col("pop_5_17") + col("pop_65_plus")) / 
             (col("pop_18_24") + col("pop_25_44") + col("pop_45_64")) * 100),
            2
        )
    ).withColumn(
        "elderly_ratio",
        spark_round((col("pop_65_plus") / col("total_population") * 100), 2)
    ).withColumn(
        "youth_ratio",
        spark_round(((col("pop_0_4") + col("pop_5_17")) / col("total_population") * 100), 2)
    ).select(
        "state_code",
        "state_name",
        "fips_code",
        "county_name",
        "year", 
        "total_population",
        "male_population",
        "female_population",
        "working_age_population",
        "prev_year_population",
        "yoy_growth_rate",
        "yoy_growth_absolute",
        "pct_0_4",
        "pct_5_17",
        "pct_18_24",
        "pct_25_44",
        "pct_45_64",
        "pct_65_plus",
        "pct_male",
        "pct_female",
        "dependency_ratio",
        "elderly_ratio",
        "youth_ratio"
    ).orderBy("state_code", "fips_code", "year")
    
    # Find maximum YoY growth rate and year for each county
    max_yoy_data = pop_with_growth.filter(col("yoy_growth_rate").isNotNull()) \
        .groupBy("state_code", "state_name", "fips_code", "county_name") \
        .agg(
            spark_max(struct(col("yoy_growth_rate"), col("year"))).alias("max_yoy_struct")
        ) \
        .select(
            col("state_code"),
            col("state_name"),
            col("fips_code"),
            col("county_name"),
            col("max_yoy_struct.yoy_growth_rate").alias("max_yoy_growth"),
            col("max_yoy_struct.year").alias("max_yoy_year")
        )
    
    # Find minimum YoY growth rate and year for each county
    min_yoy_data = pop_with_growth.filter(col("yoy_growth_rate").isNotNull()) \
        .groupBy("state_code", "state_name", "fips_code", "county_name") \
        .agg(
            spark_min(struct(col("yoy_growth_rate"), col("year"))).alias("min_yoy_struct")
        ) \
        .select(
            col("state_code"),
            col("state_name"),
            col("fips_code"),
            col("county_name"),
            col("min_yoy_struct.yoy_growth_rate").alias("min_yoy_growth"),
            col("min_yoy_struct.year").alias("min_yoy_year")
        )
    
    # Calculate average YoY growth rate for each county
    avg_yoy_data = pop_with_growth.filter(col("yoy_growth_rate").isNotNull()) \
        .groupBy("state_code", "state_name", "fips_code", "county_name") \
        .agg(
            spark_round(avg("yoy_growth_rate"), 2).alias("average_yoy_growth")
        )
    
    # Calculate population peak and decline metrics
    peak_window = Window.partitionBy("fips_code").orderBy("year").rowsBetween(Window.unboundedPreceding, 0)
    
    decline_df = pop_df.withColumn(
        "peak_population",
        spark_max("total_population").over(peak_window)
    ).withColumn(
        "decline_pct",
        spark_round(
            ((col("total_population") - col("peak_population")) / col("peak_population") * 100),
            2
        )
    )
    
    max_decline_data = decline_df.filter(col("decline_pct") < 0) \
        .groupBy("state_code", "state_name", "fips_code", "county_name") \
        .agg(
            spark_min(struct(col("decline_pct"), col("year"))).alias("max_decline_struct")
        ) \
        .select(
            col("state_code"),
            col("state_name"),
            col("fips_code"),
            col("county_name"),
            col("max_decline_struct.decline_pct").alias("maximum_decline"),
            col("max_decline_struct.year").alias("maximum_decline_year")
        )
    
    # Get first and last year population for each county
    first_last_pop = pop_df.groupBy("state_code", "state_name", "fips_code", "county_name").agg(
        spark_min(struct(col("year"), col("total_population"))).alias("first_struct"),
        spark_max(struct(col("year"), col("total_population"))).alias("last_struct")
    ).select(
        col("state_code"),
        col("state_name"),
        col("fips_code"),
        col("county_name"),
        col("first_struct.year").alias("first_year"),
        col("first_struct.total_population").alias("first_year_population"),
        col("last_struct.year").alias("last_year"),
        col("last_struct.total_population").alias("last_year_population")
    ).withColumn(
        "total_growth_pct",
        spark_round(
            ((col("last_year_population") - col("first_year_population")) / col("first_year_population") * 100),
            2
        )
    )
    
    # Calculate average demographic ratios for each county
    avg_demographics = pop_with_growth.groupBy("state_code", "state_name", "fips_code", "county_name").agg(
        spark_round(avg("dependency_ratio"), 2).alias("avg_dependency_ratio"),
        spark_round(avg("elderly_ratio"), 2).alias("avg_elderly_ratio"),
        spark_round(avg("youth_ratio"), 2).alias("avg_youth_ratio"),
        spark_round(avg("pct_male"), 2).alias("avg_pct_male"),
        spark_round(avg("pct_female"), 2).alias("avg_pct_female")
    )
    
    # Aggregate county-level summary with all growth metrics
    county_summary = first_last_pop \
        .join(avg_yoy_data, ["state_code", "state_name", "fips_code", "county_name"], "left") \
        .join(max_yoy_data, ["state_code", "state_name", "fips_code", "county_name"], "left") \
        .join(min_yoy_data, ["state_code", "state_name", "fips_code", "county_name"], "left") \
        .join(max_decline_data, ["state_code", "state_name", "fips_code", "county_name"], "left") \
        .join(avg_demographics, ["state_code", "state_name", "fips_code", "county_name"], "left") \
        .select(
            "state_code",
            "state_name",
            "fips_code",
            "county_name",
            "first_year",
            "first_year_population",
            "last_year",
            "last_year_population",
            "total_growth_pct",
            "average_yoy_growth",
            "max_yoy_growth",
            "max_yoy_year",
            "min_yoy_growth",
            "min_yoy_year",
            "maximum_decline",
            "maximum_decline_year",
            "avg_dependency_ratio",
            "avg_elderly_ratio",
            "avg_youth_ratio",
            "avg_pct_male",
            "avg_pct_female"
        ) \
        .orderBy("state_code", "fips_code")
    
    # State-level summary with basic county counts and date range
    state_summary = pop_df.groupBy("state_code", "state_name").agg(
        countDistinct("fips_code").alias("num_counties"),
        spark_min("year").alias("first_year"),
        spark_max("year").alias("last_year")
    )
    
    # State-level growth statistics aggregated from counties
    state_growth = pop_with_growth.filter(col("yoy_growth_rate").isNotNull()) \
        .groupBy("state_code", "state_name") \
        .agg(
            spark_round(avg("yoy_growth_rate"), 2).alias("avg_county_yoy_growth"),
            spark_max("yoy_growth_rate").alias("max_county_yoy_growth"),
            spark_min("yoy_growth_rate").alias("min_county_yoy_growth")
        )
    
    # Calculate average growth per county for ranking
    county_avg_growth = pop_with_growth.filter(col("yoy_growth_rate").isNotNull()) \
        .groupBy("state_code", "state_name", "fips_code", "county_name") \
        .agg(spark_round(avg("yoy_growth_rate"), 2).alias("avg_growth"))
    
    # Identify best performing county per state by average growth
    window_best = Window.partitionBy("state_code").orderBy(col("avg_growth").desc())
    window_worst = Window.partitionBy("state_code").orderBy(col("avg_growth").asc())
    
    best_counties = county_avg_growth.withColumn("rank", row_number().over(window_best)) \
        .filter(col("rank") == 1) \
        .select(
            col("state_code"),
            col("county_name").alias("fastest_growing_county"),
            col("avg_growth").alias("fastest_growth_rate")
        )
    
    # Identify worst performing county per state by average growth
    worst_counties = county_avg_growth.withColumn("rank", row_number().over(window_worst)) \
        .filter(col("rank") == 1) \
        .select(
            col("state_code"),
            col("county_name").alias("slowest_growing_county"),
            col("avg_growth").alias("slowest_growth_rate")
        )
    
    state_summary = state_summary \
        .join(state_growth, ["state_code", "state_name"], "left") \
        .join(best_counties, ["state_code"], "left") \
        .join(worst_counties, ["state_code"], "left") \
        .orderBy("state_code")
    
    # State-level population totals by year
    state_year_pop = pop_df.groupBy("state_code", "state_name", "year").agg(
        countDistinct("fips_code").alias("number_of_counties"),
        spark_sum("total_population").alias("total_population"),
        spark_sum("male_population").alias("total_male"),
        spark_sum("female_population").alias("total_female"),
        spark_sum("pop_0_4").alias("total_0_4"),
        spark_sum("pop_5_17").alias("total_5_17"),
        spark_sum("pop_18_24").alias("total_18_24"),
        spark_sum("pop_25_44").alias("total_25_44"),
        spark_sum("pop_45_64").alias("total_45_64"),
        spark_sum("pop_65_plus").alias("total_65_plus")
    )
    
    # Calculate state-level percentages
    state_year_pop = state_year_pop.withColumn(
        "pct_male",
        spark_round((col("total_male") / col("total_population") * 100), 2)
    ).withColumn(
        "pct_female",
        spark_round((col("total_female") / col("total_population") * 100), 2)
    ).withColumn(
        "pct_elderly",
        spark_round((col("total_65_plus") / col("total_population") * 100), 2)
    ).withColumn(
        "pct_working_age",
        spark_round(((col("total_18_24") + col("total_25_44") + col("total_45_64")) / col("total_population") * 100), 2)
    )
    
    county_year_pop = pop_df.select(
        "state_code", "state_name", "fips_code", "county_name", "year", "total_population"
    )
    
    # Identify largest and smallest counties per state per year by population
    window_largest_year = Window.partitionBy("state_code", "year").orderBy(col("total_population").desc())
    window_smallest_year = Window.partitionBy("state_code", "year").orderBy(col("total_population").asc())
    
    largest_counties_year = county_year_pop.withColumn("rank", row_number().over(window_largest_year)) \
        .filter(col("rank") == 1) \
        .select(
            col("state_code"),
            col("year"),
            col("county_name").alias("largest_county"),
            col("total_population").alias("largest_county_population")
        )
    
    smallest_counties_year = county_year_pop.withColumn("rank", row_number().over(window_smallest_year)) \
        .filter(col("rank") == 1) \
        .select(
            col("state_code"),
            col("year"),
            col("county_name").alias("smallest_county"),
            col("total_population").alias("smallest_county_population")
        )
    
    pop_by_state_by_year = state_year_pop \
        .join(largest_counties_year, ["state_code", "year"], "left") \
        .join(smallest_counties_year, ["state_code", "year"], "left") \
        .select(
            "state_name",
            "state_code",
            "year",
            "number_of_counties",
            "total_population",
            "pct_male",
            "pct_female",
            "pct_elderly",
            "pct_working_age",
            "largest_county",
            "largest_county_population",
            "smallest_county",
            "smallest_county_population"
        ) \
        .orderBy("state_code", "year")
    
    return pop_by_year, pop_with_growth, county_summary, state_summary, pop_by_state_by_year


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
    bronze_path = f"{base_path}/bronze/population_county"
    gold_path = f"{base_path}/gold"
    fips_mapping_path = f"{base_path}/fips_mapping.csv"
    
    try:
        if not os.path.exists(bronze_path):
            raise ValueError(f"Bronze table not found: {bronze_path}")
        
        print("Loading population data from bronze layer...")
        pop_df = process_population_data(spark, bronze_path, fips_mapping_path)
        print(f"Population records loaded: {pop_df.count():,}")
        
        print("Calculating population statistics...")
        pop_by_year, pop_with_growth, county_summary, state_summary, pop_by_state_by_year = calculate_population_statistics(pop_df)
        
        print("Saving results to gold layer...")
        save_to_parquet(pop_by_year, f"{gold_path}/population_by_year")
        print(f"  - population_by_year: {pop_by_year.count():,} records")
        
        save_to_parquet(pop_with_growth, f"{gold_path}/population_with_growth")
        print(f"  - population_with_growth: {pop_with_growth.count():,} records")
        
        save_to_parquet(county_summary, f"{gold_path}/population_county_summary")
        print(f"  - population_county_summary: {county_summary.count():,} records")
        
        save_to_parquet(state_summary, f"{gold_path}/population_state_summary")
        print(f"  - population_state_summary: {state_summary.count():,} records")
        
        save_to_parquet(pop_by_state_by_year, f"{gold_path}/population_by_state_by_year")
        print(f"  - population_by_state_by_year: {pop_by_state_by_year.count():,} records")

        print(f"\nAll Parquet files saved to: {gold_path}")
        print("Population processing completed successfully!")
        
    except Exception as e:
        print(f"Error during processing: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

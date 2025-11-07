#!/usr/bin/env python3
"""
Hello World PySpark Job
A simple example that demonstrates basic Spark operations and event logging.
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime


def create_spark_session(app_name: str) -> SparkSession:
    """Create and configure Spark session with event logging enabled."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///opt/spark/spark-events")
        .getOrCreate()
    )
    return spark


def run_hello_world(spark: SparkSession, output_path: str):
    """Run a simple Spark job that creates sample data and performs transformations."""
    
    print("=" * 50)
    print("Starting Hello World Spark Job")
    print("=" * 50)
    
    # Create sample data
    data = [
        ("Alice", 25, "Engineering"),
        ("Bob", 30, "Sales"),
        ("Charlie", 35, "Engineering"),
        ("Diana", 28, "Marketing"),
        ("Eve", 32, "Engineering"),
    ]
    
    columns = ["name", "age", "department"]
    
    # Create DataFrame
    df = spark.createDataFrame(data, columns)
    
    print("\n📊 Original Data:")
    df.show()
    
    # Perform some transformations
    print("\n🔧 Transformations:")
    
    # Add a new column
    df_transformed = df.withColumn(
        "senior", 
        F.when(F.col("age") >= 30, "Yes").otherwise("No")
    )
    
    print("\n1. Added 'senior' column (age >= 30):")
    df_transformed.show()
    
    # Group by department and calculate statistics
    print("\n2. Department Statistics:")
    dept_stats = df_transformed.groupBy("department").agg(
        F.count("*").alias("employee_count"),
        F.avg("age").alias("avg_age"),
        F.max("age").alias("max_age"),
        F.min("age").alias("min_age")
    ).orderBy(F.desc("employee_count"))
    
    dept_stats.show()
    
    # Save the results
    print(f"\n💾 Saving results to: {output_path}")
    # Use coalesce(1) to write from a single partition to avoid distributed write issues
    df_transformed.coalesce(1).write.mode("overwrite").parquet(output_path)
    
    # Verify the save
    print("\n✅ Verifying saved data:")
    saved_df = spark.read.parquet(output_path)
    print(f"Saved {saved_df.count()} records")
    saved_df.show()
    
    print("\n" + "=" * 50)
    print("Hello World Spark Job Completed Successfully! 🎉")
    print("=" * 50)
    
    return df_transformed.count()


def main():
    """Main entry point for the Spark job."""
    parser = argparse.ArgumentParser(description="Hello World PySpark Job")
    parser.add_argument(
        "--output-path",
        type=str,
        default="/opt/spark/data/hello_world_output",
        help="Output path for results"
    )
    parser.add_argument(
        "--app-name",
        type=str,
        default="HelloWorldSpark",
        help="Spark application name"
    )
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session(args.app_name)
    
    try:
        # Run the job
        record_count = run_hello_world(spark, args.output_path)
        print(f"\n✨ Processed {record_count} records")
        
    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")
        raise
    finally:
        # Stop Spark session
        spark.stop()
        print("\n🛑 Spark session stopped")


if __name__ == "__main__":
    main()

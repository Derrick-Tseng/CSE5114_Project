from pyspark.sql import SparkSession
import os

def main():
    """Validate the existence of gold tables generated in the previous task."""
    spark = SparkSession.builder.appName("validate-gold-tables").getOrCreate()
    base_path = "/opt/spark/data/gold"

    tables = [
        "gdp_by_year",
        "gdp_with_growth", 
        "gdp_county_summary",
        "gdp_state_summary",
        "gdp_by_state_by_year"
    ]

    all_valid = True

    for table in tables:
        table_path = f"{base_path}/{table}"
        if os.path.exists(table_path):
            df = spark.read.format("parquet") \
                .load(f"file://{os.path.abspath(table_path)}")
            count = df.count()
            print(f"{table}: {count:,} records")
            
            if count == 0:
                print(f"  WARNING: Table is empty!")
                all_valid = False
        else:
            print(f"{table}: NOT FOUND")
            all_valid = False

    if not all_valid:
        exit(1)

    spark.stop()

if __name__ == "__main__":
    main()

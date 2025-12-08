from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
import os
import json


def load_local_json(input_path):
    """Load local JSON file without requiring shared storage across Spark workers."""
    if not os.path.exists(input_path):
        print(f"Input path {input_path} does not exist; skipping processing.")
        return []
    with open(input_path, 'r') as source:
        try:
            data = json.load(source)
        except json.JSONDecodeError as exc:
            print(f"Failed to parse JSON from {input_path}: {exc}")
            return []
    if isinstance(data, dict):
        data = [data]
    return data


def write_local_json(records, output_dir):
    """Persist processed records locally so downstream tasks can read them."""
    if not records:
        print("No processed records to write.")
        return

    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "data.json")
    with open(output_file, "w") as sink:
        json.dump(records, sink)
    print(f"Wrote {len(records)} processed records to {output_file}")

def create_spark_session():
    """Create Spark session."""
    spark = SparkSession.builder \
        .appName("Realtime-AirQuality-Process") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def process_data(input_path, output_path):
    print(f"Reading data from {input_path}")
    records = load_local_json(input_path)
    if not records:
        print("No data to process.")
        return

    spark = create_spark_session()
    df = spark.createDataFrame(records)

    # Clean up column names (Standardize to Upper Case)
    # Spark columns are case insensitive but let's be explicit if needed
    # The JSON keys are what they are.
    
    # Rename columns to upper case and replace spaces
    for c in df.columns:
        df = df.withColumnRenamed(c, c.upper().replace(' ', '_'))
    
    # Handle the nested 'Category' dictionary if it exists
    if 'CATEGORY' in df.columns:
        # Assuming Category is a struct
        df = df.withColumn('CATEGORY_NAME', col('CATEGORY.Name')) \
               .withColumn('CATEGORY_NUM', col('CATEGORY.Number')) \
               .drop('CATEGORY')
        
    print(f"Processed {df.count()} rows.")
    df.show(5)
    
    processed_records = [row.asDict(recursive=True) for row in df.collect()]
    print(f"Writing processed data to {output_path}")
    write_local_json(processed_records, output_path)

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit realtime_aq_process.py <input_path> <output_path>")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    process_data(input_path, output_path)

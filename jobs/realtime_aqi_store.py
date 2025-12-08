from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import sys
import os
import base64
import json
def load_processed_records(processed_dir):
    """Read processed records that were written locally by the process job."""
    data_file = processed_dir
    if os.path.isdir(processed_dir):
        data_file = os.path.join(processed_dir, "data.json")

    if not os.path.exists(data_file):
        print(f"Processed data file {data_file} does not exist.")
        return []

    with open(data_file, "r") as source:
        try:
            data = json.load(source)
        except json.JSONDecodeError as exc:
            print(f"Failed to parse processed data JSON: {exc}")
            return []

    if isinstance(data, dict):
        data = [data]
    return data


def create_spark_session():
    """Create Spark session."""
    spark = SparkSession.builder \
        .appName("Realtime-AirQuality-Store") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_snowflake_options():
    """Read Snowflake configuration from environment variables."""
    sf_options = {
        "sfURL": os.getenv("SF_URL", ""),
        "sfUser": os.getenv("SF_USER", ""),
        "sfDatabase": os.getenv("SF_DATABASE", "GDP_DATA"),
        "sfSchema": os.getenv("SF_SCHEMA", "PUBLIC"),
        "sfWarehouse": os.getenv("SF_WAREHOUSE", "COMPUTE_WH"),
    }
    
    password = os.getenv("SF_PASSWORD", "")
    if password:
        sf_options["sfPassword"] = password
    
    private_key_b64 = os.getenv("SF_PRIVATE_KEY_B64", "")
    if private_key_b64:
        try:
            private_key_pem = base64.b64decode(private_key_b64).decode('utf-8')
            private_key_pem = private_key_pem.strip()
            
            lines = private_key_pem.split('\n')
            key_content = []
            in_key = False
            
            for line in lines:
                line = line.strip()
                if 'BEGIN' in line and 'PRIVATE KEY' in line:
                    in_key = True
                    continue
                elif 'END' in line and 'PRIVATE KEY' in line:
                    in_key = False
                    continue
                elif in_key and line:
                    key_content.append(line)
            
            clean_key = ''.join(key_content)
            sf_options["pem_private_key"] = clean_key
            
            passphrase = os.getenv("SF_PRIVATE_KEY_PASSPHRASE", "")
            if passphrase:
                sf_options["sfPassword"] = passphrase
                
        except Exception as e:
            print(f"Warning: Error processing private key: {str(e)}")
    
    authenticator = os.getenv("SF_AUTHENTICATOR", "snowflake")
    if authenticator and authenticator != "snowflake":
        sf_options["sfAuthenticator"] = authenticator
    
    return sf_options


def validate_snowflake_options(sf_options):
    """Ensure required Snowflake configuration is present before writing."""
    required_keys = ["sfURL", "sfUser", "sfDatabase", "sfSchema", "sfWarehouse"]
    missing = [key for key in required_keys if not sf_options.get(key)]
    if missing:
        raise ValueError(f"Missing Snowflake configuration values: {', '.join(missing)}")

    if not sf_options.get("pem_private_key") and not sf_options.get("sfPassword"):
        raise ValueError("Snowflake authentication not configured: provide SF_PASSWORD or SF_PRIVATE_KEY_B64")

REALTIME_AQI_SCHEMA = StructType([
    StructField("DATEOBSERVED", StringType(), True),
    StructField("HOUROBSERVED", IntegerType(), True),
    StructField("LOCALTIMEZONE", StringType(), True),
    StructField("REPORTINGAREA", StringType(), True),
    StructField("STATECODE", StringType(), True),
    StructField("LATITUDE", DoubleType(), True),
    StructField("LONGITUDE", DoubleType(), True),
    StructField("PARAMETERNAME", StringType(), True),
    StructField("AQI", IntegerType(), True),
    StructField("CATEGORY_NAME", StringType(), True),
    StructField("CATEGORY_NUM", IntegerType(), True),
    StructField("TARGETCITY", StringType(), True),
])

def ensure_snowflake_tables(spark, sf_options):
    """Create destination tables if they do not already exist."""
    tables_ddl = {
        "AIR_QUALITY_13_CITIES": """
            CREATE TABLE IF NOT EXISTS AIR_QUALITY_13_CITIES (
                DATEOBSERVED STRING,
                HOUROBSERVED INTEGER,
                LOCALTIMEZONE STRING,
                REPORTINGAREA STRING,
                STATECODE STRING,
                LATITUDE DOUBLE,
                LONGITUDE DOUBLE,
                PARAMETERNAME STRING,
                AQI INTEGER,
                CATEGORY_NAME STRING,
                CATEGORY_NUM INTEGER,
                TARGETCITY STRING
            )
        """,
        "AQI_REALTIME": """
            CREATE TABLE IF NOT EXISTS AQI_REALTIME (
                CITY STRING,
                POLLUTANT_TYPE STRING,
                VALUE INTEGER
            )
        """,
    }

    for table_name, ddl in tables_ddl.items():
        try:
            spark.read.format("snowflake") \
                .options(**sf_options) \
                .option("preActions", ddl) \
                .option("query", "SELECT 1") \
                .load()
            print(f"Ensured table exists: {table_name}")
        except Exception as exc:
            print(f"Warning: could not create table {table_name}: {exc}")


def normalize_records(records):
    """Ensure every record has all schema fields so Spark can apply the explicit schema."""
    fields = [field.name for field in REALTIME_AQI_SCHEMA]
    normalized = []
    for record in records:
        normalized.append({name: record.get(name) for name in fields})
    return normalized


def store_data(input_path):
    records = load_processed_records(input_path)
    if not records:
        print("No data to store.")
        return

    spark = create_spark_session()
    normalized_records = normalize_records(records)
    print(f"Creating Spark DataFrame with {len(normalized_records)} processed rows")
    df = spark.createDataFrame(normalized_records, schema=REALTIME_AQI_SCHEMA)

    sf_options = get_snowflake_options()
    validate_snowflake_options(sf_options)
    # ensure_snowflake_tables(spark, sf_options)
    
    # --- Write to AIR_QUALITY_13_CITIES ---
    print("Writing to Snowflake table AIR_QUALITY_13_CITIES...")
    df.write \
        .format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", "AIR_QUALITY_13_CITIES") \
        .mode("append") \
        .save()
        
    print("Successfully wrote data to AIR_QUALITY_13_CITIES.")

    # --- Write to AQI_REALTIME ---
    print("Preparing data for AQI_REALTIME...")
    
    # Select and rename columns: TargetCity -> CITY, ParameterName -> POLLUTANT_TYPE, AQI -> VALUE
    realtime_df = df.select(
        col("TARGETCITY").alias("CITY"),
        col("PARAMETERNAME").alias("POLLUTANT_TYPE"),
        col("AQI").alias("VALUE")
    )
    
    print("Writing to Snowflake table AQI_REALTIME (Overwrite mode)...")
    realtime_df.write \
        .format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", "AQI_REALTIME") \
        .mode("overwrite") \
        .save()
        
    print("Successfully wrote data to AQI_REALTIME.")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit realtime_aq_store.py <input_path>")
        sys.exit(1)
    
    input_path = sys.argv[1]
    store_data(input_path)

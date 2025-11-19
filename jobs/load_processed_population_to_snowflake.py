from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import base64


def create_spark_session():
    """Create Spark session."""
    spark = SparkSession.builder \
        .appName("Processed-Population-Load-To-Snowflake") \
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


def load_gold_table_to_snowflake(spark, gold_path, table_name, sf_options, column_mapping=None):
    """Generic function to load gold layer data to Snowflake."""
    
    print(f"\nLoading {table_name} from: {gold_path}")
    
    if not os.path.exists(gold_path):
        print(f"Warning: Gold data not found at {gold_path}")
        return
    
    df = spark.read.format("parquet").load(f"file://{os.path.abspath(gold_path)}")
    
    # Apply column mapping if provided (rename columns to match Snowflake naming conventions)
    if column_mapping:
        for old_col, new_col in column_mapping.items():
            df = df.withColumnRenamed(old_col, new_col)
    
    # Convert all column names to uppercase for Snowflake
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.upper())
    
    record_count = df.count()
    print(f"Records to load: {record_count:,}")
    
    if record_count == 0:
        print("No records to load")
        return
    
    write_options = sf_options.copy()
    write_options["dbtable"] = table_name
    write_options["truncate_table"] = "on"
    write_options["usestagingtable"] = "off"
    
    print(f"Writing to Snowflake table: {table_name}...")
    
    df.write \
        .format("snowflake") \
        .options(**write_options) \
        .mode("overwrite") \
        .save()
    
    print(f"Successfully loaded {record_count:,} records to {table_name}")


def main():
    spark = create_spark_session()
    
    try:
        sf_options = get_snowflake_options()
        
        if not sf_options.get("sfURL") or not sf_options.get("sfUser"):
            print("Snowflake credentials not configured")
            print("Set SF_URL, SF_USER, and SF_PASSWORD environment variables")
            return
        
        print("Snowflake Configuration:")
        print(f"  URL: {sf_options['sfURL']}")
        print(f"  User: {sf_options['sfUser']}")
        print(f"  Database: {sf_options['sfDatabase']}")
        print(f"  Schema: {sf_options['sfSchema']}")
        print(f"  Warehouse: {sf_options['sfWarehouse']}")
        
        base_path = "/opt/spark/data"
        gold_path = f"{base_path}/gold"
        
        # Load population_with_growth - detailed year-by-year data with growth metrics
        load_gold_table_to_snowflake(
            spark,
            f"{gold_path}/population_with_growth",
            "POPULATION_WITH_GROWTH",
            sf_options
        )
        
        # Load population_county_summary - aggregated county-level statistics
        load_gold_table_to_snowflake(
            spark,
            f"{gold_path}/population_county_summary",
            "POPULATION_COUNTY_SUMMARY",
            sf_options
        )
        
        # Load population_state_summary - aggregated state-level statistics
        load_gold_table_to_snowflake(
            spark,
            f"{gold_path}/population_state_summary",
            "POPULATION_STATE_SUMMARY",
            sf_options
        )
        
        # Load population_by_state_by_year - state totals by year
        load_gold_table_to_snowflake(
            spark,
            f"{gold_path}/population_by_state_by_year",
            "POPULATION_BY_STATE_BY_YEAR",
            sf_options
        )
        
        print("\n" + "="*60)
        print("Processed population data load completed successfully!")
        print("="*60)
        print("\nAvailable tables in Snowflake:")
        print("  1. POPULATION_DATA - Raw population data by county and year")
        print("  2. POPULATION_WITH_GROWTH - Detailed growth metrics and demographics")
        print("  3. POPULATION_COUNTY_SUMMARY - County-level aggregated statistics")
        print("  4. POPULATION_STATE_SUMMARY - State-level aggregated statistics")
        print("  5. POPULATION_BY_STATE_BY_YEAR - State population trends by year")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

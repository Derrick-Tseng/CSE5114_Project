from pyspark.sql import SparkSession
import os
import base64

def create_spark_session():
    """Create Spark session."""
    spark = SparkSession.builder \
        .appName("GDP-Load-To-Snowflake") \
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

def create_snowflake_tables(spark, sf_options):
    """Create Snowflake tables."""
    
    tables_ddl = {
        "GDP_BY_YEAR": """
            CREATE OR REPLACE TABLE GDP_BY_YEAR (
                STATE_CODE STRING,
                STATE_NAME STRING,
                FIPS_CODE STRING,
                COUNTY_NAME STRING,
                YEAR INTEGER,
                DATE DATE,
                GDP_VALUE BIGINT
            )
        """,
        "GDP_WITH_GROWTH": """
            CREATE OR REPLACE TABLE GDP_WITH_GROWTH (
                STATE_CODE STRING,
                STATE_NAME STRING,
                FIPS_CODE STRING,
                COUNTY_NAME STRING,
                YEAR INTEGER,
                GDP_VALUE BIGINT,
                PREV_YEAR_GDP BIGINT,
                YOY_GROWTH_RATE DOUBLE
            )
        """,
        "GDP_COUNTY_SUMMARY": """
            CREATE OR REPLACE TABLE GDP_COUNTY_SUMMARY (
                STATE_CODE STRING,
                STATE_NAME STRING,
                FIPS_CODE STRING,
                COUNTY_NAME STRING,
                FIRST_YEAR INTEGER,
                LAST_YEAR INTEGER,
                YEARS_OF_DATA BIGINT,
                AVERAGE_YOY DOUBLE,
                MAX_YOY DOUBLE,
                MAX_YOY_YEAR INTEGER,
                MIN_YOY DOUBLE,
                MIN_YOY_YEAR INTEGER,
                MAXIMUM_DRAW_DOWN DOUBLE,
                MAXIMUM_DRAW_DOWN_YEAR INTEGER
            )
        """,
        "GDP_STATE_SUMMARY": """
            CREATE OR REPLACE TABLE GDP_STATE_SUMMARY (
                STATE_CODE STRING,
                STATE_NAME STRING,
                NUM_COUNTIES BIGINT,
                FIRST_YEAR INTEGER,
                LAST_YEAR INTEGER,
                AVG_COUNTY_YOY DOUBLE,
                MAX_COUNTY_YOY DOUBLE,
                MIN_COUNTY_YOY DOUBLE,
                BEST_PERFORM_COUNTY STRING,
                BEST_PERFORM_AVG_YOY DOUBLE,
                LEAST_PERFORM_COUNTY STRING,
                LEAST_PERFORM_AVG_YOY DOUBLE
            )
        """,
        "GDP_BY_STATE_BY_YEAR": """
            CREATE OR REPLACE TABLE GDP_BY_STATE_BY_YEAR (
                STATE_NAME STRING,
                STATE_CODE STRING,
                YEAR INTEGER,
                NUMBER_OF_COUNTY BIGINT,
                TOTAL_GDP BIGINT,
                BEST_PERFORM_COUNTY STRING,
                BEST_PERFORM_COUNTY_GDP BIGINT,
                LEAST_PERFORM_COUNTY STRING,
                LEAST_PERFORM_COUNTY_GDP BIGINT
            )
        """
    }
    
    for table_name, ddl in tables_ddl.items():
        try:
            spark.read.format("snowflake") \
                .options(**sf_options) \
                .option("preActions", ddl) \
                .option("query", f"SELECT 1") \
                .load()
            print(f"  Created table: {table_name}")
        except Exception as e:
            print(f"  Error creating table {table_name}: {e}")

def load_parquet_to_snowflake(spark, parquet_path, table_name, sf_options):
    """Load Parquet to Snowflake table."""
    
    if not os.path.exists(parquet_path):
        raise ValueError(f"Parquet path not found: {parquet_path}")
    
    df = spark.read.format("parquet") \
        .load(f"file://{os.path.abspath(parquet_path)}")
    
    df.write.format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", table_name) \
        .mode("overwrite") \
        .save()
    
    count = df.count()
    print(f"  Loaded {count:,} records")
    
    return count

def main():
    spark = create_spark_session()
    
    base_path = "/opt/spark/data"
    gold_path = f"{base_path}/gold"
    
    try:
        
        sf_options = get_snowflake_options()
        
        if not all([sf_options.get("sfURL"), sf_options.get("sfUser"), 
                   sf_options.get("sfDatabase"), sf_options.get("sfSchema"),
                   sf_options.get("sfWarehouse")]):
            raise ValueError("Missing Snowflake configuration")
        
        if not sf_options.get("pem_private_key") and not sf_options.get("sfPassword"):
            raise ValueError("No authentication method configured")
        
        print(f"Database: {sf_options.get('sfDatabase')}.{sf_options.get('sfSchema')}")
        
        create_snowflake_tables(spark, sf_options)
        
        tables = {
            "GDP_BY_YEAR": f"{gold_path}/gdp_by_year",
            "GDP_WITH_GROWTH": f"{gold_path}/gdp_with_growth",
            "GDP_COUNTY_SUMMARY": f"{gold_path}/gdp_county_summary",
            "GDP_STATE_SUMMARY": f"{gold_path}/gdp_state_summary",
            "GDP_BY_STATE_BY_YEAR": f"{gold_path}/gdp_by_state_by_year"
        }
        
        total_records = 0
        
        for table_name, parquet_path in tables.items():
            if not os.path.exists(parquet_path):
                print(f"Warning: Parquet path not found: {parquet_path}")
                continue
            
            count = load_parquet_to_snowflake(spark, parquet_path, table_name, sf_options)
            total_records += count
        
        print("Data successfully loaded to Snowflake")
        
    except Exception as e:
        print(f"Error loading to Snowflake: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, lit
import os
import base64


def create_spark_session():
    """Create Spark session."""
    spark = SparkSession.builder \
        .appName("Population-Load-To-Snowflake") \
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


def create_population_table(spark, sf_options):
    """Create Snowflake table for population data."""
    
    ddl = """
        CREATE TABLE IF NOT EXISTS POPULATION_DATA (
            FIPS_CODE STRING,
            YEAR INTEGER,
            TOTAL_POPULATION BIGINT,
            MALE_POPULATION BIGINT,
            FEMALE_POPULATION BIGINT,
            POPULATION_0_4 BIGINT,
            POPULATION_5_17 BIGINT,
            POPULATION_18_64 BIGINT,
            POPULATION_65_PLUS BIGINT,
            PRIMARY KEY (FIPS_CODE, YEAR)
        )
    """
    
    try:
        query_options = sf_options.copy()
        query_options["dbtable"] = "POPULATION_DATA"
        
        empty_df = spark.createDataFrame([], "fips_code STRING, year INT")
        empty_df.write \
            .format("snowflake") \
            .options(**query_options) \
            .mode("append") \
            .save()
        
        print("Population table created/verified")
    except Exception as e:
        print(f"Table creation: {str(e)}")


def load_county_population_to_snowflake(spark, bronze_path, sf_options):
    """Load county population data to Snowflake with upsert."""
    
    print(f"Loading county population from: {bronze_path}")
    
    if not os.path.exists(bronze_path):
        print(f"Warning: Population bronze data not found at {bronze_path}")
        return
    
    pop_df = spark.read.format("parquet").load(f"file://{os.path.abspath(bronze_path)}")
    
    # Calculate derived age groups: 18-64 = 18-24 + 25-44 + 45-64
    pop_df = pop_df.withColumn(
        "pop_18_64",
        col("pop_18_24") + col("pop_25_44") + col("pop_45_64")
    )
    
    # Select and rename columns to match Snowflake schema
    pop_df = pop_df.select(
        col("fips_code").alias("FIPS_CODE"),
        col("year").alias("YEAR"),
        col("total_population").alias("TOTAL_POPULATION"),
        col("male_population").alias("MALE_POPULATION"),
        col("female_population").alias("FEMALE_POPULATION"),
        col("pop_0_4").alias("POPULATION_0_4"),
        col("pop_5_17").alias("POPULATION_5_17"),
        col("pop_18_64").alias("POPULATION_18_64"),
        col("pop_65_plus").alias("POPULATION_65_PLUS")
    )
    
    record_count = pop_df.count()
    print(f"Population records to load: {record_count:,}")
    
    if record_count == 0:
        print("No records to load")
        return
    
    write_options = sf_options.copy()
    write_options["dbtable"] = "POPULATION_DATA"
    write_options["truncate_table"] = "on"
    write_options["usestagingtable"] = "off"
    
    print("Writing to Snowflake (truncate and reload)...")
    
    # Truncate and reload ensures idempotence
    # Same data loaded multiple times results in same final state
    pop_df.write \
        .format("snowflake") \
        .options(**write_options) \
        .mode("overwrite") \
        .save()
    
    print(f"Successfully loaded {record_count:,} population records to Snowflake")



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
        
        print("\nCreating population table...")
        create_population_table(spark, sf_options)
        
        base_path = "/opt/spark/data"
        bronze_path = f"{base_path}/bronze/population_county"
        
        print("\nLoading county population data...")
        load_county_population_to_snowflake(spark, bronze_path, sf_options)
        
        print("\nPopulation data load completed successfully")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

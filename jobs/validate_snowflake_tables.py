from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import base64

def create_spark_session():
    spark = SparkSession.builder \
        .appName("validate-snowflake-tables") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_snowflake_config():
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

def validate_table(spark, table_name, sf_options, expected_min_records=0):
    """Validate Snowflake table."""
    try:
        print(f"\nValidating {table_name}...")
        
        df = spark.read.format("snowflake") \
            .options(**sf_options) \
            .option("dbtable", table_name) \
            .load()
        
        count = df.count()
        print(f"  Table exists: {count:,} records")
        
        if count == 0:
            print(f"  ERROR: Table is empty")
            return False
        
        if expected_min_records > 0 and count < expected_min_records:
            print(f"  ERROR: Expected at least {expected_min_records:,} records")
            return False
        
        null_checks_passed = True
        
        if table_name == "GDP_BY_YEAR":
            key_cols = ["FIPS_CODE", "YEAR", "GDP_VALUE"]
        elif table_name == "GDP_WITH_GROWTH":
            key_cols = ["FIPS_CODE", "YEAR", "GDP_VALUE"]
        elif table_name == "GDP_COUNTY_SUMMARY":
            key_cols = ["FIPS_CODE", "COUNTY_NAME", "YEARS_OF_DATA"]
        elif table_name == "GDP_STATE_SUMMARY":
            key_cols = ["STATE_CODE", "STATE_NAME", "NUM_COUNTIES"]
        elif table_name == "GDP_BY_STATE_BY_YEAR":
            key_cols = ["STATE_CODE", "YEAR", "TOTAL_GDP"]
        else:
            key_cols = []
        
        for col_name in key_cols:
            if col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                if null_count > 0:
                    print(f"  WARNING: {null_count:,} null values in {col_name}")
                    null_checks_passed = False
                else:
                    print(f"  No nulls in {col_name}")
        
        return null_checks_passed and count > 0
        
    except Exception as e:
        print(f"  ERROR validating {table_name}: {str(e)}")
        return False

def main():
    spark = create_spark_session()
    
    try:
        
        sf_options = get_snowflake_config()
        
        if not all([sf_options.get("sfURL"), sf_options.get("sfUser"), 
                   sf_options.get("sfDatabase"), sf_options.get("sfSchema"),
                   sf_options.get("sfWarehouse")]):
            raise ValueError("Missing Snowflake configuration")
        
        # if sf_options.get("pem_private_key"):
        #     print(f"Authentication: Key-pair")
        # elif sf_options.get("sfPassword"):
        #     print(f"Authentication: Password")
        # else:
        #     raise ValueError("No authentication method configured")

        if not sf_options.get("pem_private_key") and not sf_options.get("sfPassword"):
            raise ValueError("No authentication method configured")
        
        print(f"Database: {sf_options.get('sfDatabase')}.{sf_options.get('sfSchema')}")
        
        tables_to_validate = {
            "GDP_BY_YEAR": 50000,
            "GDP_WITH_GROWTH": 45000,
            "GDP_COUNTY_SUMMARY": 3000,
            "GDP_STATE_SUMMARY": 50,
            "GDP_BY_STATE_BY_YEAR": 800
        }
        
        all_valid = True
        validation_results = {}
        
        for table_name, min_records in tables_to_validate.items():
            is_valid = validate_table(spark, table_name, sf_options, min_records)
            validation_results[table_name] = is_valid
            
            if not is_valid:
                all_valid = False
        
        print("\nVALIDATION SUMMARY")
        
        for table_name, is_valid in validation_results.items():
            status = "PASSED" if is_valid else "FAILED"
            print(f"{table_name}: {status}")
        
        if not all_valid:
            print("\nValidation FAILED")
            exit(1)
        else:
            print("\nAll validations PASSED")
        
    except Exception as e:
        print(f"\nError during validation: {str(e)}")
        import traceback
        traceback.print_exc()
        exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

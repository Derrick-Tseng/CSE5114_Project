#!/usr/bin/env python3
"""
Hello World PySpark Job with Snowflake Integration
A simple example that demonstrates basic Spark operations and writing to Snowflake.
"""
import argparse
import os
import base64
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime


def create_spark_session(app_name: str, snowflake_packages: str = None) -> SparkSession:
    """Create and configure Spark session with event logging and Snowflake support."""
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///opt/spark/spark-events")
    )
    
    # Add Snowflake packages if provided
    if snowflake_packages:
        builder = builder.config("spark.jars.packages", snowflake_packages)
    
    spark = builder.getOrCreate()
    return spark


def get_snowflake_options():
    """Read Snowflake configuration from environment variables."""
    sf_options = {
        "sfURL": os.getenv("SF_URL", ""),
        "sfUser": os.getenv("SF_USER", ""),
        "sfDatabase": os.getenv("SF_DATABASE", ""),
        "sfSchema": os.getenv("SF_SCHEMA", ""),
        "sfWarehouse": os.getenv("SF_WAREHOUSE", ""),
    }
    
    # Check for password authentication
    password = os.getenv("SF_PASSWORD", "")
    if password:
        sf_options["sfPassword"] = password
    
    # Check for key-pair authentication (SSH mode)
    private_key_b64 = os.getenv("SF_PRIVATE_KEY_B64", "")
    if private_key_b64:
        try:
            # Decode the base64-encoded private key
            private_key_pem = base64.b64decode(private_key_b64).decode('utf-8')
            
            # Remove any extra whitespace and ensure proper formatting
            private_key_pem = private_key_pem.strip()
            
            # The Snowflake connector expects the key without the header/footer
            # Extract just the key content
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
            
            # Join the key content back together
            clean_key = ''.join(key_content)
            
            # Set the private key for Snowflake
            sf_options["pem_private_key"] = clean_key
            
            # Add passphrase if provided
            passphrase = os.getenv("SF_PRIVATE_KEY_PASSPHRASE", "")
            if passphrase:
                sf_options["sfPassword"] = passphrase
                
        except Exception as e:
            print(f"⚠️  Warning: Error processing private key: {str(e)}")
            print("Falling back to password authentication if available")
    
    # Optional: Add role if specified
    role = os.getenv("SNOWFLAKE_ROLE", "")
    if role:
        sf_options["sfRole"] = role
    
    # Add authenticator (default to snowflake, or externalbrowser for SSO)
    authenticator = os.getenv("SF_AUTHENTICATOR", "snowflake")
    if authenticator and authenticator != "snowflake":
        sf_options["sfAuthenticator"] = authenticator
    
    return sf_options


def run_hello_world(spark: SparkSession, output_path: str, write_to_snowflake: bool = False, 
                    snowflake_table: str = "HELLO_WORLD_DATA"):
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
    
    # Add timestamp column
    df_transformed = df_transformed.withColumn(
        "processed_at",
        F.current_timestamp()
    )
    
    print("\n1. Added 'senior' column (age >= 30) and 'processed_at' timestamp:")
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
    
    # Save the results to local file
    print(f"\n💾 Saving results to local path: {output_path}")
    df_transformed.coalesce(1).write.mode("overwrite").parquet(output_path)
    
    # Verify the save
    print("\n✅ Verifying saved data:")
    saved_df = spark.read.parquet(output_path)
    print(f"Saved {saved_df.count()} records")
    saved_df.show()
    
    # Write to Snowflake if enabled
    if write_to_snowflake:
        print("\n" + "=" * 50)
        print("Writing data to Snowflake")
        print("=" * 50)
        
        try:
            sf_options = get_snowflake_options()
            
            # Validate Snowflake configuration
            if not all([sf_options.get("sfURL"), sf_options.get("sfUser"), 
                       sf_options.get("sfDatabase"), sf_options.get("sfSchema"),
                       sf_options.get("sfWarehouse")]):
                print("⚠️  Warning: Missing Snowflake configuration. Skipping Snowflake write.")
                print("Please set SF_URL, SF_USER, SF_DATABASE, SF_SCHEMA, and SF_WAREHOUSE in .env")
            else:
                print(f"🔗 Connecting to Snowflake: {sf_options.get('sfURL')}")
                print(f"� User: {sf_options.get('sfUser')}")
                print(f"🏢 Warehouse: {sf_options.get('sfWarehouse')}")
                print(f"�📊 Target table: {sf_options.get('sfDatabase')}.{sf_options.get('sfSchema')}.{snowflake_table}")
                
                # Check authentication method
                if sf_options.get("pem_private_key"):
                    print(f"🔐 Authentication: Key-pair (SSH mode)")
                    print(f"   Key length: {len(sf_options.get('pem_private_key', ''))} characters")
                elif sf_options.get("sfPassword"):
                    print(f"🔐 Authentication: Password")
                else:
                    print(f"⚠️  Warning: No authentication method configured!")
                
                # Write to Snowflake
                print("\n📤 Writing data to Snowflake...")
                df_transformed.write \
                    .format("snowflake") \
                    .options(**sf_options) \
                    .option("dbtable", snowflake_table) \
                    .mode("overwrite") \
                    .save()
                
                print(f"✅ Successfully wrote {df_transformed.count()} records to Snowflake table: {snowflake_table}")
                
                # Verify write by reading back from Snowflake
                print("\n🔍 Verifying data in Snowflake:")
                df_from_snowflake = spark.read \
                    .format("snowflake") \
                    .options(**sf_options) \
                    .option("dbtable", snowflake_table) \
                    .load()
                
                print(f"Read {df_from_snowflake.count()} records from Snowflake")
                df_from_snowflake.show()
                
        except Exception as e:
            print(f"❌ Error writing to Snowflake: {str(e)}")
            print("\n🔍 Troubleshooting tips:")
            print("1. Verify your private key is properly registered in Snowflake:")
            print(f"   ALTER USER {sf_options.get('sfUser')} SET RSA_PUBLIC_KEY='<your_public_key>';")
            print("2. Ensure the private key is in PKCS#8 PEM format")
            print("3. Check that the warehouse is running and accessible")
            print("4. Verify database and schema permissions")
            import traceback
            traceback.print_exc()
            print("\nContinuing with local output only...")
    
    print("\n" + "=" * 50)
    print("Hello World Spark Job Completed Successfully! 🎉")
    print("=" * 50)
    
    return df_transformed.count()


def main():
    """Main entry point for the Spark job."""
    parser = argparse.ArgumentParser(description="Hello World PySpark Job with Snowflake")
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
    parser.add_argument(
        "--write-snowflake",
        action="store_true",
        help="Write data to Snowflake"
    )
    parser.add_argument(
        "--snowflake-table",
        type=str,
        default="HELLO_WORLD_DATA",
        help="Snowflake table name"
    )
    parser.add_argument(
        "--snowflake-packages",
        type=str,
        default=os.getenv("SPARK_PACKAGES", "net.snowflake:spark-snowflake_2.12:3.1.5"),
        help="Snowflake Spark connector packages"
    )
    
    args = parser.parse_args()
    
    # Check WRITE_SNOWFLAKE environment variable
    write_to_sf = args.write_snowflake or os.getenv("WRITE_SNOWFLAKE", "false").lower() == "true"
    
    # Create Spark session with Snowflake packages if writing to Snowflake
    packages = args.snowflake_packages if write_to_sf else None
    spark = create_spark_session(args.app_name, packages)
    
    try:
        # Run the job
        record_count = run_hello_world(
            spark, 
            args.output_path, 
            write_to_snowflake=write_to_sf,
            snowflake_table=args.snowflake_table
        )
        print(f"\n✨ Processed {record_count} records")
        
    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        # Stop Spark session
        spark.stop()
        print("\n🛑 Spark session stopped")


if __name__ == "__main__":
    main()

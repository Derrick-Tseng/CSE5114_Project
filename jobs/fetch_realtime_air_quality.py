from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col
import requests
import pandas as pd
import os
import base64

def create_spark_session():
    """Create Spark session."""
    spark = SparkSession.builder \
        .appName("Realtime-AirQuality-Fetch-And-Load") \
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
            
            sf_options["pem_private_key"] = "".join(key_content)
        except Exception as e:
            print(f"Error processing private key: {str(e)}")
            
    return sf_options

def fetch_air_quality_data():
    # --- CONFIGURATION ---
    API_KEY = "CF66B41B-D089-41BF-B588-28E54412BAC7" 
    RADIUS_MILES = 50        
    
    # Define our target cities with their central coordinates
    cities = {
        "New York City":  {"lat": 40.7128, "lon": -74.0060},
        "Washington DC":  {"lat": 38.9047, "lon": -77.0163},
        "San Francisco":  {"lat": 37.7749, "lon": -122.4194},
        "Los Angeles":    {"lat": 34.0522, "lon": -118.2437},
        "Chicago":        {"lat": 41.8781, "lon": -87.6298},
        "Houston":        {"lat": 29.7604, "lon": -95.3698},
        "Miami":          {"lat": 25.7617, "lon": -80.1918},
        "Minneapolis":    {"lat": 44.9778, "lon": -93.2650},
        "Denver":         {"lat": 39.7392, "lon": -104.9903},
        "Seattle":        {"lat": 47.6062, "lon": -122.3321},
        "Atlanta":        {"lat": 33.7490, "lon": -84.3880},
        "Boston":         {"lat": 42.3601, "lon": -71.0589},
        "St. Louis":      {"lat": 38.6270, "lon": -90.1994}
    }
    
    base_url = "https://www.airnowapi.org/aq/observation/latLong/current/"
    all_data = []

    # --- LOOP THROUGH CITIES ---
    print("Fetching data from AirNow API...")
    for city_name, coords in cities.items():
        params = {
            "format": "application/json",
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "distance": RADIUS_MILES,
            "API_KEY": API_KEY
        }
        
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # If we got data, add a "TargetCity" column so we know which one it belongs to
            if data:
                for row in data:
                    row['TargetCity'] = city_name
                all_data.extend(data)
                
        except Exception as e:
            # If one city fails, print error but keep going to the next city
            print(f"Failed to fetch {city_name}: {e}")
            continue

    if not all_data:
        print("No data found for any of the requested cities.")
        return None

    # --- PROCESS DATA ---
    df = pd.DataFrame(all_data)
    
    # Clean up column names (Standardize to Upper Case)
    df.columns = [c.upper().replace(' ','_') for c in df.columns] 
    
    # Handle the nested 'Category' dictionary if it exists
    if 'CATEGORY' in df.columns:
        df['CATEGORY_NAME'] = df['CATEGORY'].apply(lambda x: x.get('Name') if isinstance(x, dict) else None)
        df['CATEGORY_NUM'] = df['CATEGORY'].apply(lambda x: x.get('Number') if isinstance(x, dict) else None)
        df = df.drop(columns=['CATEGORY'])
        
    return df

def main():
    spark = create_spark_session()
    
    # Fetch data using pandas first (easier for API calls)
    pdf = fetch_air_quality_data()
    
    if pdf is None or pdf.empty:
        print("No data to write.")
        return

    # Convert to Spark DataFrame
    # We might need to handle schema inference or explicit schema if data types are tricky
    # For now, let Spark infer from Pandas
    df = spark.createDataFrame(pdf)
    
    print(f"Fetched {df.count()} rows.")
    df.show(5)
    
    # Write to Snowflake
    sf_options = get_snowflake_options()
    
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
    # Note: Column names in df are already upper-cased and spaces replaced by underscores
    
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

if __name__ == "__main__":
    main()

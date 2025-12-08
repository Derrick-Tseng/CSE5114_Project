CREATE OR REPLACE NETWORK RULE airnow_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('www.airnowapi.org');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION airnow_integration
  ALLOWED_NETWORK_RULES = (airnow_network_rule)
  ENABLED = true;

GRANT USAGE ON INTEGRATION airnow_integration TO ROLE SYSADMIN;

CREATE OR REPLACE PROCEDURE fetch_13_cities_airnow()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'main'
EXTERNAL_ACCESS_INTEGRATIONS = (airnow_integration)
PACKAGES = ('snowflake-snowpark-python', 'requests', 'pandas')
AS
$$
import requests
import pandas as pd
from snowflake.snowpark import Session
import time

def main(session: Session):
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
        return "No data found for any of the requested cities."

    # --- SAVE TO SNOWFLAKE ---
    df = pd.DataFrame(all_data)
    
    # Clean up column names (Standardize to Upper Case)
    df.columns = [c.upper().replace(' ','_') for c in df.columns] 
    
    # Handle the nested 'Category' dictionary if it exists
    if 'CATEGORY' in df.columns:
        df['CATEGORY_NAME'] = df['CATEGORY'].apply(lambda x: x.get('Name') if isinstance(x, dict) else None)
        df['CATEGORY_NUM'] = df['CATEGORY'].apply(lambda x: x.get('Number') if isinstance(x, dict) else None)
        df = df.drop(columns=['CATEGORY'])

    snowpark_df = session.create_dataframe(df)
    snowpark_df.write.mode("append").save_as_table("AIR_QUALITY_13_CITIES")

    return f"Success! Pulled data for {len(cities)} cities. Total rows: {len(df)}"
$$;

CALL fetch_13_cities_airnow();

SELECT * FROM AIR_QUALITY_13_CITIES;

-- scheduler for each 60 minutes
CREATE OR REPLACE TASK pull_air_quality_hourly
  WAREHOUSE = GATOR_WH 
  SCHEDULE = '60 MINUTE'
AS
  CALL fetch_13_cities_airnow();

ALTER TASK pull_air_quality_hourly RESUME;
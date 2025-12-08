import requests
import json
import sys
import os
import boto3
from botocore.exceptions import BotoCoreError, NoCredentialsError

def fetch_air_quality_data(output_path):
    api_key = os.getenv('REALTIME_AQI_API_KEY')
    if not api_key:
        raise RuntimeError("REALTIME_AQI_API_KEY is not set")

    radius_miles = int(os.getenv('REALTIME_AQI_RADIUS_MILES', 50))
    request_timeout = int(os.getenv('REALTIME_AQI_REQUEST_TIMEOUT', 30))
    
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

    for city_name, coords in cities.items():
        params = {
            "format": "application/json",
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "distance": radius_miles,
            "API_KEY": api_key
        }
        
        try:
            response = requests.get(base_url, params=params, timeout=request_timeout)
            response.raise_for_status()
            data = response.json()
            if data:
                for row in data:
                    row['TargetCity'] = city_name
                all_data.extend(data)
                
        except Exception as e:
            continue

    if not all_data:
        return

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, 'w') as f:
        json.dump(all_data, f)

    upload_to_s3(output_path)

def upload_to_s3(local_path):
    bucket = os.getenv('REALTIME_AQI_S3_BUCKET') or os.getenv('AWS_S3_BUCKET_NAME')
    if not bucket:
        raise RuntimeError("No S3 bucket configured for realtime AQI uploads")

    prefix = os.getenv('REALTIME_AQI_S3_PREFIX', 'raw/realtime_aqi').strip('/')
    folder = os.path.basename(os.path.dirname(local_path))
    filename = os.path.basename(local_path)

    key_parts = [prefix] if prefix else []
    if folder:
        key_parts.append(folder)
    key_parts.append(filename)
    key = '/'.join(part for part in key_parts if part)

    try:
        boto3.client('s3').upload_file(local_path, bucket, key)
    except (NoCredentialsError, BotoCoreError) as exc:
        raise RuntimeError(f"Failed to upload AQI file to S3: {exc}") from exc
    except Exception as exc:
        raise RuntimeError(f"Unexpected error uploading AQI file to S3: {exc}") from exc

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python realtime_aq_fetch.py <output_path>")
        sys.exit(1)
    
    output_path = sys.argv[1]
    fetch_air_quality_data(output_path)

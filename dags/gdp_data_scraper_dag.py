"""
GDP Data Scraper Airflow DAG
This DAG downloads GDP data from FRED (Federal Reserve Economic Data) for all valid FIPS codes.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import csv
import os


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def scrape_gdp_data():
    """
    Download GDP data from FRED for all valid FIPS codes.
    Returns statistics about the download process.
    """
    data_path = "/opt/airflow/data/gdp/"
    fips_csv_path = "/opt/airflow/valid_fips_codes.csv"

    if not os.path.exists(fips_csv_path):
        raise FileNotFoundError(f"valid_fips_codes.csv not found. Checked paths: /opt/airflow/valid_fips_codes.csv")
    
    # Read validated FIPS codes from CSV (only real counties/cities)
    city_code = []
    with open(fips_csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            city_code.append(f"gdpall{row['fips_code']}")
    
    print(f"Total files to download: {len(city_code)}")
    successful = 0
    failed = 0
    invalid_codes = []
    
    for i, code in enumerate(city_code, 1):
        url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23ebf3fb&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1320&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id={code}&scale=left&cosd=2001-01-01&coed=2023-01-01&line_color=%230073e6&link_values=false&line_style=solid&mark_type=none&mw=3&lw=3&ost=-99999&oet=99999&mma=0&fml=a&fq=Annual&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2025-11-02&revision_date=2025-11-02&nd=2001-01-01"
        dest_path = data_path + f"{code}.csv"
        
        try:
            with requests.get(url, stream=True, timeout=15) as r:
                r.raise_for_status()
                with open(dest_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            successful += 1
            if i % 100 == 0:
                print(f"Progress: {i}/{len(city_code)} - Downloaded: {successful}, Failed: {failed}")
        except requests.exceptions.HTTPError as e:
            # Skip invalid FIPS codes (404 errors) and mark for removal
            failed += 1
            invalid_codes.append(code.replace("gdpall", ""))  # Store just the FIPS code
            if i % 100 == 0:
                print(f"Progress: {i}/{len(city_code)} - Downloaded: {successful}, Failed: {failed}")
        except Exception as e:
            print(f"Error downloading {code}: {e}")
            failed += 1
            invalid_codes.append(code.replace("gdpall", ""))
    
    print(f"\nDownload complete!")
    print(f"Successfully downloaded: {successful}")
    print(f"Failed/Invalid FIPS codes: {failed}")
    
    # Remove invalid FIPS codes from CSV
    if invalid_codes:
        print(f"\nRemoving {len(invalid_codes)} invalid FIPS codes from CSV...")
        
        # Read all valid codes
        valid_codes = []
        with open(fips_csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['fips_code'] not in invalid_codes:
                    valid_codes.append(row['fips_code'])
        
        # Write back only valid codes
        with open(fips_csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['fips_code'])
            for fips in valid_codes:
                writer.writerow([fips])
        
        print(f"Updated valid_fips_codes.csv with {len(valid_codes)} valid FIPS codes")


def print_start():
    """Print a start message."""
    print("=" * 50)
    print("🚀 Starting GDP Data Scraper")
    print("=" * 50)


def print_completion(**context):
    """Print a completion message."""
    print("=" * 50)
    print("✅ GDP Data Scraper Completed Successfully!")
    print("=" * 50)


# Create the DAG
with DAG(
    dag_id='gdp_data_scraper',
    default_args=default_args,
    description='Download GDP data from FRED for all valid FIPS codes',
    schedule_interval='@yearly',  # Run yearly to get updated data
    catchup=False,
    tags=['data-collection', 'gdp', 'fred', 'scraper'],
) as dag:
    
    # Task 1: Print start message
    start_task = PythonOperator(
        task_id='print_start',
        python_callable=print_start,
    )
    
    # Task 2: Scrape GDP data from FRED
    scrape_task = PythonOperator(
        task_id='scrape_gdp_data',
        python_callable=scrape_gdp_data,
    )
    
    # Task 3: Print completion message with statistics
    end_task = PythonOperator(
        task_id='print_completion',
        python_callable=print_completion,
    )
    
    # Define task dependencies
    start_task >> scrape_task >> end_task

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import requests
import csv
import os


class GDPDownloadOperator(BaseOperator):
    """
    Download GDP data from FRED with FIPS codes.
    
    :param data_path: Where GDP CSV files will be saved
    :param fips_csv_path: CSV file with valid FIPS codes
    :param timeout: Request timeout in seconds
    :param update_invalid_codes: Remove invalid FIPS codes from CSV (When unable to download)
    """
    
    @apply_defaults
    def __init__(
        self,
        data_path='/opt/airflow/data/gdp',
        fips_csv_path='/opt/airflow/data/valid_fips_codes.csv',
        timeout=15,
        update_invalid_codes=False,
        s3_bucket=None,
        s3_prefix='raw/gdp/',
        **kwargs
    ):
        super(GDPDownloadOperator, self).__init__(**kwargs)
        self.data_path = data_path
        self.fips_csv_path = fips_csv_path
        self.timeout = timeout
        self.update_invalid_codes = update_invalid_codes
        self.s3_bucket = s3_bucket or os.getenv('AWS_S3_BUCKET_NAME')
        prefix = os.getenv('GDP_S3_PREFIX', s3_prefix)
        self.s3_prefix = (prefix.rstrip('/') + '/') if prefix else ''
    
    def execute(self, context):
        self.log.info(f"Starting GDP data download to {self.data_path}")

        if not self.s3_bucket:
            raise AirflowException("S3 bucket not configured. Set AWS_S3_BUCKET_NAME or pass s3_bucket explicitly.")
        
        os.makedirs(self.data_path, exist_ok=True)
        
        if not os.path.exists(self.fips_csv_path):
            raise FileNotFoundError(f"FIPS codes csv not found at {self.fips_csv_path}")
        
        city_code = []
        with open(self.fips_csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                city_code.append(f"gdpall{row['fips_code']}")
        
        self.log.info(f"Total files to download: {len(city_code)}")
        successful = 0
        failed = 0
        invalid_codes = []
        
        # Initialize S3 client once if bucket is provided
        s3_client = None
        try:
            import boto3
            s3_client = boto3.client('s3')
        except Exception as e:
            raise AirflowException(f"Failed to initialize S3 client: {e}") from e

        for i, code in enumerate(city_code, 1):
            url = self._build_fred_url(code)
            dest_path = os.path.join(self.data_path, f"{code}.csv")
            
            try:
                with requests.get(url, stream=True, timeout=self.timeout) as r:
                    r.raise_for_status()
                    with open(dest_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                
                try:
                    key = f"{self.s3_prefix}{code}.csv"
                    s3_client.upload_file(dest_path, self.s3_bucket, key)
                except Exception as e:
                    raise AirflowException(f"Error uploading {code}.csv to S3: {e}") from e

                successful += 1
                
                if i % 100 == 0:
                    self.log.info(f"Progress: {i}/{len(city_code)} - Downloaded: {successful}, Failed: {failed}")
                    
            except requests.exceptions.HTTPError as e:
                self.log.warning(f"HTTP error for {code}: {e}")
                failed += 1
                invalid_codes.append(code.replace("gdpall", ""))
                
            except Exception as e:
                self.log.error(f"Error downloading {code}: {e}")
                failed += 1
                invalid_codes.append(code.replace("gdpall", ""))
        
        self.log.info("Download complete")
        self.log.info(f"Successfully downloaded: {successful}")
        self.log.info(f"Failed/Invalid FIPS codes: {failed}")
        
        if self.update_invalid_codes and invalid_codes:
            self._update_fips_csv(invalid_codes)
        
        stats = {
            'successful': successful,
            'failed': failed,
            'total': len(city_code),
            'invalid_codes': invalid_codes,
            'remaining_valid_codes': successful
        }
        context['task_instance'].xcom_push(key='download_stats', value=stats)
        
        return successful
    
    def _build_fred_url(self, code):
        """Build FRED API URL for GDP data."""
        base_url = "https://fred.stlouisfed.org/graph/fredgraph.csv"
        params = (
            "?bgcolor=%23ebf3fb&chart_type=line&drp=0&fo=open%20sans"
            "&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on"
            "&txtcolor=%23444444&ts=12&tts=12&width=1320&nt=0&thu=0&trc=0"
            "&show_legend=yes&show_axis_titles=yes&show_tooltip=yes"
            f"&id={code}&scale=left&cosd=2001-01-01&coed=2023-01-01"
            "&line_color=%230073e6&link_values=false&line_style=solid"
            "&mark_type=none&mw=3&lw=3&ost=-99999&oet=99999&mma=0&fml=a"
            "&fq=Annual&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1"
            "&transformation=lin&vintage_date=2025-11-02&revision_date=2025-11-02"
            "&nd=2001-01-01"
        )
        return base_url + params
    
    def _update_fips_csv(self, invalid_codes):
        """Remove invalid FIPS codes from CSV."""
        self.log.info(f"Removing {len(invalid_codes)} invalid FIPS codes from CSV")
        
        valid_codes = []
        with open(self.fips_csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['fips_code'] not in invalid_codes:
                    valid_codes.append(row['fips_code'])
        
        with open(self.fips_csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['fips_code'])
            for fips in valid_codes:
                writer.writerow([fips])
        
        self.log.info(f"Updated CSV with {len(valid_codes)} valid FIPS codes")

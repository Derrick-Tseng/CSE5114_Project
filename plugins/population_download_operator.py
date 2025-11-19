from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
import os
import zipfile


class PopulationDownloadOperator(BaseOperator):
    """
    Download population data from StatsAmerica.
    
    :param data_path: Where the ZIP file will be saved and extracted
    :param url: URL to download the population data from
    :param timeout: Request timeout in seconds
    :param extract: Whether to extract the ZIP file after download
    """
    
    @apply_defaults
    def __init__(
        self,
        data_path='/opt/airflow/data/population',
        url='https://www.statsamerica.org/downloads/Population-by-Age-and-Sex.zip',
        timeout=60,
        extract=True,
        **kwargs
    ):
        super(PopulationDownloadOperator, self).__init__(**kwargs)
        self.data_path = data_path
        self.url = url
        self.timeout = timeout
        self.extract = extract
    
    def execute(self, context):
        self.log.info(f"Starting population data download to {self.data_path}")
        
        os.makedirs(self.data_path, exist_ok=True)
        
        zip_filename = os.path.basename(self.url)
        zip_path = os.path.join(self.data_path, zip_filename)
        
        try:
            self.log.info(f"Downloading from {self.url}")
            
            with requests.get(self.url, stream=True, timeout=self.timeout) as r:
                r.raise_for_status()
                
                total_size = int(r.headers.get('content-length', 0))
                downloaded = 0
                
                with open(zip_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            
                            if total_size > 0 and downloaded % (1024 * 1024) == 0:
                                progress = (downloaded / total_size) * 100
                                self.log.info(f"Download progress: {progress:.1f}%")
            
            file_size_mb = os.path.getsize(zip_path) / (1024 * 1024)
            self.log.info(f"Successfully downloaded {zip_filename} ({file_size_mb:.2f} MB)")
            
            if self.extract:
                self.log.info(f"Extracting {zip_filename}")
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(self.data_path)
                    extracted_files = zip_ref.namelist()
                    self.log.info(f"Extracted {len(extracted_files)} files")
                    
                stats = {
                    'zip_file': zip_filename,
                    'file_size_mb': file_size_mb,
                    'extracted_files': extracted_files,
                    'extract_path': self.data_path
                }
            else:
                stats = {
                    'zip_file': zip_filename,
                    'file_size_mb': file_size_mb,
                    'zip_path': zip_path
                }
            
            context['task_instance'].xcom_push(key='download_stats', value=stats)
            return True
            
        except requests.exceptions.RequestException as e:
            self.log.error(f"Error downloading population data: {e}")
            raise
        except zipfile.BadZipFile as e:
            self.log.error(f"Error extracting ZIP file: {e}")
            raise
        except Exception as e:
            self.log.error(f"Unexpected error: {e}")
            raise

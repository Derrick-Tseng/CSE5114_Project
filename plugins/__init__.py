"""
Register custom operators.
"""
from gdp_download_operator import GDPDownloadOperator
from spark_submit_operator import SparkSubmitOperator

__all__ = ['GDPDownloadOperator', 'SparkSubmitOperator']
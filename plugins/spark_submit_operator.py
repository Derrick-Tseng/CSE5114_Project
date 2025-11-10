from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import subprocess
import logging


class SparkSubmitOperator(BaseOperator):
    """
    Customized spark submit pperator.
    
    :param application: Path to Spark application
    :param master: Spark master URL
    :param deploy_mode: Deploy mode
    :param driver_memory: Driver memory
    :param executor_memory: Executor memory
    :param executor_cores: Number of executor cores
    :param spark_conf: Additional Spark configurations
    :param packages: Maven coordinates of jars to include
    :param jars: Local jars to include
    """
    
    @apply_defaults
    def __init__(
        self,
        application,
        master='spark://spark-master:7077',
        deploy_mode='client',
        driver_memory='2g',
        executor_memory='2g',
        executor_cores=2,
        spark_conf=None,
        packages=None,
        jars=None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.application = application
        self.master = master
        self.deploy_mode = deploy_mode
        self.driver_memory = driver_memory
        self.executor_memory = executor_memory
        self.executor_cores = executor_cores
        self.spark_conf = spark_conf or {}
        self.packages = packages
        self.jars = jars
        
    def execute(self, context):
        cmd = [
            '/opt/spark/bin/spark-submit',
            '--master', self.master,
            '--deploy-mode', self.deploy_mode,
            '--driver-memory', self.driver_memory,
            '--executor-memory', self.executor_memory,
            '--executor-cores', str(self.executor_cores),
        ]
        
        if self.packages:
            cmd.extend(['--packages', self.packages])
        
        if self.jars:
            cmd.extend(['--jars', self.jars])
        
        for key, value in self.spark_conf.items():
            cmd.extend(['--conf', f'{key}={value}'])
        
        cmd.append(self.application)
        
        self.log.info(f"Executing: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True
            )
            
            if result.stdout:
                self.log.info("STDOUT:")
                for line in result.stdout.split('\n'):
                    if line.strip():
                        self.log.info(line)
            
            if result.stderr:
                self.log.info("STDERR:")
                for line in result.stderr.split('\n'):
                    if line.strip():
                        self.log.info(line)
            
            self.log.info("Spark job completed")
            return result.returncode
            
        except subprocess.CalledProcessError as e:
            self.log.error(f"Spark job failed with code {e.returncode}")
            
            if e.stdout:
                self.log.error("STDOUT:")
                for line in e.stdout.split('\n'):
                    if line.strip():
                        self.log.error(line)
            
            if e.stderr:
                self.log.error("STDERR:")
                for line in e.stderr.split('\n'):
                    if line.strip():
                        self.log.error(line)
            
            raise AirflowException(f"Spark job failed: {str(e)}")

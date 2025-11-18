from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os


class FileCleanupOperator(BaseOperator):
    """
    Remove specific files from a directory.
    
    :param data_path: Directory path where files are located
    :param files_to_remove: List of filenames to delete
    """
    
    @apply_defaults
    def __init__(
        self,
        data_path,
        files_to_remove,
        **kwargs
    ):
        super(FileCleanupOperator, self).__init__(**kwargs)
        self.data_path = data_path
        self.files_to_remove = files_to_remove
    
    def execute(self, context):
        self.log.info(f"Starting file cleanup in {self.data_path}")
        
        if not os.path.exists(self.data_path):
            self.log.warning(f"Directory does not exist: {self.data_path}")
            return {'removed': 0, 'not_found': len(self.files_to_remove)}
        
        removed = []
        not_found = []
        
        for filename in self.files_to_remove:
            file_path = os.path.join(self.data_path, filename)
            if os.path.exists(file_path):
                os.remove(file_path)
                removed.append(filename)
                self.log.info(f"Deleted {filename}")
            else:
                not_found.append(filename)
                self.log.warning(f"File not found: {filename}")
        
        self.log.info(f"Cleanup complete - Removed: {len(removed)}, Not found: {len(not_found)}")
        
        stats = {
            'removed': len(removed),
            'not_found': len(not_found),
            'removed_files': removed,
            'not_found_files': not_found
        }
        
        context['task_instance'].xcom_push(key='cleanup_stats', value=stats)
        
        return len(removed)

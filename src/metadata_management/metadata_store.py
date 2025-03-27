import logging
import json
import os
from typing import Dict, Any, List
from datetime import datetime

class MetadataStore:
    def __init__(self, storage_path: str):
        """Initialize metadata store with storage path"""
        self.logger = logging.getLogger(__name__)
        self.storage_path = storage_path
        self._metadata = {}
        
        # Create both tables and projects directories
        self.tables_path = os.path.join(storage_path, "tables")
        self.projects_path = os.path.join(storage_path, "projects")
        os.makedirs(self.tables_path, exist_ok=True)
        os.makedirs(self.projects_path, exist_ok=True)
        
        # Load any existing metadata
        self._load_metadata()

    def _get_project_dir(self, project_id: str) -> str:
        """Get project directory path"""
        return os.path.join(self.projects_path, project_id)

    def _get_file_path(self, catalog: str, schema: str, table: str) -> str:
        """Generate file path for metadata storage"""
        filename = f"{catalog}.{schema}.{table}.json"
        return os.path.join(self.tables_path, filename)

    def _get_tables_metadata_path(self, project_id: str) -> str:
        """Get path for tables metadata summary file"""
        project_dir = self._get_project_dir(project_id)
        os.makedirs(project_dir, exist_ok=True)
        return os.path.join(project_dir, "tables_metadata.json")

    def _update_tables_metadata(self, project_id: str, catalog: str, schema: str, table: str, metadata: Dict[str, Any]) -> None:
        """Update the tables metadata summary file"""
        try:
            summary_path = self._get_tables_metadata_path(project_id)
            summary_data = {}
            
            # Load existing summary if it exists
            if os.path.exists(summary_path):
                with open(summary_path, 'r') as f:
                    summary_data = json.load(f)
            
            # Update summary with new table metadata
            table_key = f"{catalog}.{schema}.{table}"
            summary_data[table_key] = {
                'catalog': catalog,
                'schema': schema,
                'table': table,
                'columns': [
                    {
                        'name': col['name'],
                        'type': col['type'],
                        'nullable': col['nullable']
                    } for col in metadata.get('columns', [])
                ],
                'row_count': metadata.get('row_count'),
                'last_updated': metadata.get('last_updated'),
                'has_sample_data': bool(metadata.get('sample_data')),
                'description': metadata.get('description', ''),
                'sample_data': metadata.get('sample_data', [])[:2]  # Include first 2 rows of sample data
            }
            
            # Save updated summary
            with open(summary_path, 'w') as f:
                json.dump(summary_data, f, indent=2)
                
        except Exception as e:
            self.logger.error(f"Failed to update tables metadata summary: {str(e)}")
            raise

    def _load_metadata(self) -> None:
        """Load existing metadata from storage"""
        try:
            for filename in os.listdir(self.storage_path):
                if filename.endswith('.json'):
                    file_path = os.path.join(self.storage_path, filename)
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        key = filename[:-5]  # Remove .json extension
                        self._metadata[key] = data
        except Exception as e:
            self.logger.error(f"Failed to load metadata from storage: {str(e)}")

    def store_metadata(
        self,
        project_id: str,
        connection_id: str,
        catalog: str,
        schema: str,
        table: str,
        metadata: Dict[str, Any]
    ) -> None:
        """Store metadata for a table"""
        try:
            # Store table metadata in tables directory
            table_file_path = self._get_file_path(catalog, schema, table)
            with open(table_file_path, 'w') as f:
                json.dump(metadata, f, indent=2)

            # Update project's tables metadata summary
            project_dir = self._get_project_dir(project_id)
            os.makedirs(project_dir, exist_ok=True)
            tables_metadata_path = os.path.join(project_dir, "tables_metadata.json")
            
            # Load existing or create new tables metadata
            tables_metadata = {}
            if os.path.exists(tables_metadata_path):
                with open(tables_metadata_path, 'r') as f:
                    tables_metadata = json.load(f)
            
            # Update with new table metadata
            table_key = f"{catalog}.{schema}.{table}"
            tables_metadata[table_key] = {
                'catalog': catalog,
                'schema': schema,
                'table': table,
                'columns': [
                    {
                        'name': col['name'],
                        'type': col['type'],
                        'nullable': col.get('nullable', True),
                        'description': col.get('description', '')
                    } for col in metadata.get('columns', [])
                ],
                'description': metadata.get('description', ''),
                'row_count': metadata.get('general_info', {}).get('row_count'),
                'last_updated': datetime.now().isoformat()
            }
            
            # Save updated tables metadata
            with open(tables_metadata_path, 'w') as f:
                json.dump(tables_metadata, f, indent=2)

            self.logger.debug(f"Stored metadata for {catalog}.{schema}.{table}")

        except Exception as e:
            self.logger.error(f"Failed to store metadata for {catalog}.{schema}.{table}: {str(e)}")
            raise

    def _update_table_summary(
        self,
        project_id: str,
        catalog: str,
        schema: str,
        table: str,
        metadata: Dict[str, Any]
    ) -> None:
        """Update the summary file for a project's tables"""
        try:
            summary_path = os.path.join(self._get_project_dir(project_id), "tables_summary.json")
            
            # Load existing summary or create new
            summary_data = {}
            if os.path.exists(summary_path):
                with open(summary_path, 'r') as f:
                    summary_data = json.load(f)
            
            # Update summary with new table metadata
            table_key = f"{catalog}.{schema}.{table}"
            summary_data[table_key] = {
                'catalog': catalog,
                'schema': schema,
                'table': table,
                'columns': [
                    {
                        'name': col['name'],
                        'type': col['type'],
                        'nullable': col['nullable'],
                        'description': col.get('description', '')
                    } for col in metadata.get('columns', [])
                ],
                'row_count': metadata.get('row_count'),
                'last_updated': metadata.get('last_updated'),
                'has_sample_data': bool(metadata.get('sample_data')),
                'description': metadata.get('description', '')
            }
            
            # Save updated summary
            with open(summary_path, 'w') as f:
                json.dump(summary_data, f, indent=2)
                
        except Exception as e:
            self.logger.error(f"Failed to update tables metadata summary: {str(e)}")
            raise

    def get_metadata(self, project_id: str, connection_id: str, catalog: str, schema: str, table: str) -> Dict[str, Any]:
        """Retrieve metadata for a table"""
        try:
            file_path = self._get_file_path(catalog, schema, table)
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            self.logger.error(f"Failed to retrieve metadata for {catalog}.{schema}.{table}: {str(e)}")
            raise

    def list_tables(self, project_id: str, connection_id: str = None) -> List[Dict[str, Any]]:
        """List all tables for a project"""
        tables = []
        try:
            for key, metadata in self._metadata.items():
                parts = key.split('_')
                if len(parts) >= 5 and parts[0] == project_id:
                    if connection_id is None or parts[1] == connection_id:
                        tables.append({
                            'catalog': parts[2],
                            'schema': parts[3],
                            'table': parts[4],
                            'metadata': metadata
                        })
            return tables
        except Exception as e:
            self.logger.error(f"Failed to list tables for project {project_id}: {str(e)}")
            raise

    def delete_metadata(self, project_id: str, connection_id: str, catalog: str, schema: str, table: str) -> None:
        """Delete metadata for a table"""
        try:
            key = f"{project_id}_{connection_id}_{catalog}_{schema}_{table}"
            if key in self._metadata:
                del self._metadata[key]
                
                # Delete file if it exists
                file_path = self._get_file_path(project_id, connection_id, catalog, schema, table)
                if os.path.exists(file_path):
                    os.remove(file_path)
                    
            self.logger.debug(f"Deleted metadata for {key}")
        except Exception as e:
            self.logger.error(f"Failed to delete metadata for {catalog}.{schema}.{table}: {str(e)}")
            raise

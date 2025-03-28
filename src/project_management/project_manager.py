import os
import json
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
import logging
import uuid

from src.metadata_management.metadata_store import MetadataStore  # Fixed import path

class Project:
    def __init__(self, id: str, name: str, description: str, skip_llm: bool = False, 
                 metadata_store: Optional[MetadataStore] = None):
        self.id = id
        self.name = name
        self.description = description
        self.skip_llm = skip_llm
        current_time = datetime.now(timezone.utc).isoformat()
        self.created_at = current_time
        self.updated_at = current_time
        self.data_sources = []
        self.data_sources_count = 0
        self.metadata_store = metadata_store

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "skip_llm": self.skip_llm,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "data_sources": self.data_sources,
            "data_sources_count": self.data_sources_count
        }

class ProjectManager:
    def __init__(self, storage_path: str):
        # Current issue: Inconsistent path joining
        self.storage_path = os.path.join(storage_path, "projects")
        # Should standardize path handling across all methods
        self.logger = logging.getLogger(__name__)
        os.makedirs(self.storage_path, exist_ok=True)

    def save_project(self, project: Project) -> Dict[str, Any]:
        """Save a project to storage"""
        try:
            project_dir = os.path.join(self.storage_path, project.id)
            os.makedirs(project_dir, exist_ok=True)
            
            # Save project metadata in project.json
            project_file = os.path.join(project_dir, "project.json")
            project_data = project.to_dict()
            
            with open(project_file, 'w') as f:
                json.dump(project_data, f, indent=2)
            
            return project_data
        except Exception as e:
            self.logger.error(f"Failed to save project: {str(e)}")
            raise

    def get_project(self, project_id: str) -> Dict[str, Any]:
        """Get project details by ID"""
        if not project_id:
            raise ValueError("Project ID cannot be empty")
            
        try:
            project_file = os.path.join(self.storage_path, project_id, "project.json")
            if not os.path.exists(project_file):
                raise FileNotFoundError(f"Project {project_id} not found")
            
            with open(project_file, 'r') as f:
                project_data = json.load(f)
                
            # Validate required fields
            required_fields = ['id', 'name', 'description', 'created_at']
            missing_fields = [field for field in required_fields if field not in project_data]
            if missing_fields:
                raise ValueError(f"Project data missing required fields: {missing_fields}")
                
            return project_data
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in project file: {str(e)}")
            raise ValueError(f"Project file contains invalid JSON: {str(e)}")
        except Exception as e:
            self.logger.error(f"Failed to get project: {str(e)}")
            raise

    def list_projects(self) -> List[Dict[str, Any]]:
        """List all projects"""
        try:
            projects = []
            for project_id in os.listdir(self.storage_path):
                project_file = os.path.join(self.storage_path, project_id, "project.json")
                if os.path.exists(project_file):
                    with open(project_file, 'r') as f:
                        projects.append(json.load(f))
            return projects
        except Exception as e:
            self.logger.error(f"Failed to list projects: {str(e)}")
            raise

    def delete_project(self, project_id: str) -> bool:
        """Delete a project"""
        try:
            project_dir = os.path.join(self.storage_path, project_id)
            if os.path.exists(project_dir):
                import shutil
                shutil.rmtree(project_dir)
                return True
            return False
        except Exception as e:
            self.logger.error(f"Failed to delete project: {str(e)}")
            raise

    def update_project(self, project_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        """Update project details"""
        try:
            project_data = self.get_project(project_id)
            project_data.update(updates)
            project_data['updated_at'] = datetime.now().isoformat()
            
            project_file = os.path.join(self.storage_path, project_id, "project.json")
            with open(project_file, 'w') as f:
                json.dump(project_data, f, indent=2)
            
            return project_data
        except Exception as e:
            self.logger.error(f"Failed to update project: {str(e)}")
            raise

    def update_tables_metadata(
        self, 
        project_id: str, 
        table_key: str, 
        metadata: Dict[str, Any]
    ) -> None:
        """Update tables metadata for a project
        
        Args:
            project_id: Unique identifier of the project
            table_key: Unique identifier of the table
            metadata: Dictionary containing table metadata
            
        Raises:
            FileNotFoundError: If project directory doesn't exist
            ValueError: If metadata is invalid
            IOError: If file operations fail
        """
        try:
            metadata_file = os.path.join(self.storage_path, project_id, "tables_metadata.json")
            
            # Load existing metadata
            if os.path.exists(metadata_file):
                with open(metadata_file, 'r') as f:
                    tables_metadata = json.load(f)
            else:
                tables_metadata = {}
            
            # Update metadata for the specific table
            tables_metadata[table_key] = metadata
            
            # Save updated metadata
            os.makedirs(os.path.dirname(metadata_file), exist_ok=True)
            with open(metadata_file, 'w') as f:
                json.dump(tables_metadata, f, indent=2)
        
        except Exception as e:
            self.logger.error(f"Failed to update tables metadata: {str(e)}")
            raise

    def add_data_source(
        self,
        project_id: str,
        connection_id: str,
        catalog: str,
        schema: str,
        tables: List[str],
        metadata_store: MetadataStore
    ) -> Dict[str, Any]:
        """Add a data source to a project and manage its metadata
        
        Args:
            project_id (str): Project ID
            connection_id (str): Connection ID
            catalog (str): Catalog name
            schema (str): Schema name
            tables (List[str]): List of tables
            metadata_store (MetadataStore): Metadata store instance
        
        Returns:
            Dict[str, Any]: Results of data source addition
        """
        try:
            # Create data source record
            data_source = {
                "id": str(uuid.uuid4()),
                "connection_id": connection_id,
                "catalog": catalog,
                "schema": schema,
                "tables": tables,
                "created_at": datetime.utcnow().isoformat()
            }

            # Load and update project data
            project_file = os.path.join(self.storage_path, project_id, "project.json")
            if not os.path.exists(project_file):
                raise ValueError(f"Project {project_id} not found")
            
            with open(project_file, 'r') as f:
                project_data = json.load(f)
            
            if 'data_sources' not in project_data:
                project_data['data_sources'] = []
            
            project_data['data_sources'].append(data_source)
            project_data['data_sources_count'] = len(project_data['data_sources'])
            project_data['updated_at'] = datetime.utcnow().isoformat()
            
            # Save updated project data
            with open(project_file, 'w') as f:
                json.dump(project_data, f, indent=2)

            return {
                'status': 'success',
                'data_source': data_source,
                'project_id': project_id
            }

        except Exception as e:
            logging.error(f"Failed to add data source to project: {str(e)}")
            return {
                'status': 'error',
                'error': str(e)
            }

    def get_project_metadata(self, project_id: str) -> Dict[str, Any]:
        """Get consolidated metadata for a project including all its data sources"""
        try:
            # Get project details
            project_data = self.get_project(project_id)
            if not project_data:
                raise ValueError(f"Project {project_id} not found")

            # Get tables metadata
            metadata_file = os.path.join(self.storage_path, project_id, "tables_metadata.json")
            tables_metadata = {}
            if os.path.exists(metadata_file):
                with open(metadata_file, 'r') as f:
                    tables_metadata = json.load(f)

            return {
                'project_id': project_id,
                'project_name': project_data.get('name'),
                'project_description': project_data.get('description'),
                'data_sources': project_data.get('data_sources', []),
                'tables': tables_metadata,
                'timestamp': datetime.utcnow().isoformat()
            }

        except Exception as e:
            self.logger.error(f"Failed to get project metadata: {str(e)}")
            raise

import uuid
from datetime import datetime  # Change this import
from typing import List, Dict, Any, Optional
from fastapi import HTTPException
import logging
import os
from dotenv import load_dotenv

from src.trino_connection.connection_manager import TrinoConnectionManager
from src.trino_connection.connection_config import ConnectionStorage, SecureConnectionStorage
from src.project_management.project_manager import ProjectManager, Project
from src.metadata_management.metadata_store import MetadataStore
from src.metadata_management.metadata_extractor import GenericMetadataExtractor, TrinoMetadataExtractor
from src.metadata_management.llm_enricher import MetadataEnricher

class ApplicationService:
    """Main service layer coordinating business logic"""
    
    def __init__(self):
        """Initialize service components"""
        load_dotenv()
        self.logger = logging.getLogger(__name__)
        
        # Set up storage paths
        storage_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            'storage',
            'metadata'
        )
        os.makedirs(storage_path, exist_ok=True)
        
        # Initialize components
        self.connection_storage = SecureConnectionStorage()  # Add this line
        self.connection_manager = None  # Will be initialized when connecting
        self.metadata_store = MetadataStore(storage_path=storage_path)
        self.project_manager = ProjectManager(storage_path=storage_path)
        self.metadata_extractor = None  # Will be initialized after connection
        
        # Initialize OpenAI client for LLM enrichment
        openai_api_key = os.getenv('OPENAI_API_KEY')
        if not openai_api_key:
            self.logger.error("OPENAI_API_KEY not set - LLM enrichment will be disabled")
            self.metadata_enricher = None
        else:
            try:
                from openai import OpenAI
                client = OpenAI(api_key=openai_api_key)
                self.metadata_enricher = MetadataEnricher(client)
                self.logger.info("OpenAI client initialized successfully")
            except Exception as e:
                self.logger.error(f"Failed to initialize OpenAI client: {e}", exc_info=True)
                self.metadata_enricher = None

    def create_project(self, name: str, description: str, skip_llm: bool = False) -> Dict[str, Any]:
        """Create a new project"""
        try:
            self.logger.info(f"Creating project '{name}' with skip_llm={skip_llm}")
            project = Project(
                id=str(uuid.uuid4()),
                name=name,
                description=description,
                skip_llm=skip_llm,
                metadata_store=self.metadata_store
            )
            return self.project_manager.save_project(project)
        except Exception as e:
            self.logger.error(f"Failed to create project: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    def list_projects(self) -> List[Dict[str, Any]]:
        """List all projects"""
        try:
            return self.project_manager.list_projects()
        except Exception as e:
            self.logger.error(f"Failed to list projects: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    def get_project(self, project_id: str) -> Dict[str, Any]:
        """Get project details"""
        try:
            return self.project_manager.get_project(project_id)
        except Exception as e:
            self.logger.error(f"Failed to get project: {str(e)}")
            raise HTTPException(status_code=404, detail=str(e))

    def get_connection(self, connection_id: str) -> TrinoConnectionManager:
        """Get an existing connection by ID"""
        try:
            connection_details = self.connection_storage.get_connection(connection_id)
            if not connection_details:
                raise ValueError(f"No connection found with ID: {connection_id}")

            connection = TrinoConnectionManager(
                host=connection_details['host'],
                port=connection_details['port'],
                user=connection_details['user'],
                password=connection_details.get('password'),
                http_scheme=connection_details.get('http_scheme', 'http'),
                verify=connection_details.get('verify', True),
                connection_storage=self.connection_storage
            )
            
            # Connect using stored catalog/schema if available
            if 'catalog' in connection_details and 'schema' in connection_details:
                connection.connect_to_catalog(
                    catalog=connection_details['catalog'],
                    schema=connection_details['schema']
                )
            else:
                connection.connect()
            
            return connection
            
        except Exception as e:
            self.logger.error(f"Failed to get connection {connection_id}: {str(e)}")
            raise

    def connect_trino(self, connection_details: Dict[str, Any]) -> Dict[str, str]:
        """Create a new Trino connection"""
        try:
            self.connection_manager = TrinoConnectionManager(
                host=connection_details['host'],
                port=connection_details['port'],
                user=connection_details['user'],
                password=connection_details.get('password'),
                http_scheme=connection_details.get('http_scheme', 'http'),
                verify=connection_details.get('verify', True)
            )
            
            # Test connection before storing
            self.connection_manager.connect()
            
            # Initialize metadata extractor after connection is established
            self.metadata_extractor = GenericMetadataExtractor(self.connection_manager)
            
            # Generate connection ID and store connection details
            connection_id = str(uuid.uuid4())
            self.connection_storage.store_connection(connection_id, connection_details)  # Add this line
            
            return {"connection_id": connection_id}
            
        except Exception as e:
            self.logger.error(f"Failed to create Trino connection: {str(e)}")
            raise HTTPException(status_code=400, detail=str(e))

    def list_trino_connections(self) -> Dict[str, Dict]:
        """List all saved Trino connections"""
        try:
            return self.connection_storage.list_connections()
        except Exception as e:
            self.logger.error(f"Failed to list Trino connections: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    def get_trino_connection_details(self, connection_id: str) -> Dict[str, Any]:
        """Get details for a specific Trino connection"""
        try:
            connections = self.connection_storage.list_connections()
            if connection_id not in connections:
                raise HTTPException(status_code=404, detail=f"Connection {connection_id} not found")
            return connections[connection_id]
        except Exception as e:
            self.logger.error(f"Failed to get Trino connection details: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    def get_trino_connection_tables(self, connection_id: str, catalog: str, schema: str) -> Dict[str, List[str]]:
        """Get available tables for a connection"""
        try:
            # Get connection details and create a new connection
            connection_details = self.connection_storage.get_connection(connection_id)
            if not connection_details:
                raise ValueError(f"No connection found with ID: {connection_id}")

            connection = TrinoConnectionManager(
                host=connection_details['host'],
                port=connection_details['port'],
                user=connection_details['user'],
                password=connection_details.get('password'),
                http_scheme=connection_details.get('http_scheme', 'http'),
                verify=connection_details.get('verify', True)
            )
            
            # Connect to the specified catalog/schema
            connection.connect()
            
            # Create metadata extractor and get tables
            extractor = GenericMetadataExtractor(connection)
            tables = extractor.get_all_tables(connection, catalog, schema)
            
            return {"available_tables": tables}
        
        except Exception as e:
            self.logger.error(f"Error getting tables for {catalog}.{schema}: {str(e)}")
            raise HTTPException(status_code=404, detail=str(e))

    def process_table_metadata(self, connection_id: str, project_id: str, catalog: str, schema: str, table: str) -> Dict[str, Any]:
        try:
            project = self.project_manager.get_project(project_id)
            skip_llm = project.get('skip_llm', False) if isinstance(project, dict) else project.skip_llm
            self.logger.info(f"Processing {catalog}.{schema}.{table} with skip_llm={skip_llm}, enricher={self.metadata_enricher is not None}")
            
            # Get connection details from storage
            connection_details = self.connection_storage.get_connection(connection_id)
            if not connection_details:
                raise ValueError(f"No connection found with ID: {connection_id}")

            # Create a connection instance
            connection = TrinoConnectionManager(
                host=connection_details['host'],
                port=connection_details['port'],
                user=connection_details['user'],
                password=connection_details.get('password'),
                http_scheme=connection_details.get('http_scheme', 'http'),
                verify=connection_details.get('verify', True),
                connection_storage=self.connection_storage
            )
            connection.connect()

            # Extract metadata using the connection object
            metadata = self.metadata_extractor.extract_table_metadata(
                connection=connection,  # Pass connection, not connection_id
                catalog=catalog,
                schema=schema,
                table=table
            )
            
            if not skip_llm and self.metadata_enricher:
                try:
                    self.logger.info(f"Starting LLM enrichment for {catalog}.{schema}.{table}")
                    table_description = self.metadata_enricher.enrich_table_metadata(
                        table_name=f"{catalog}.{schema}.{table}",
                        columns=metadata['columns'],
                        sample_data=metadata.get('sample_data', [])
                    )
                    enriched_columns = self.metadata_enricher.enrich_column_metadata(
                        table_name=f"{catalog}.{schema}.{table}",
                        columns=metadata['columns'],
                        sample_data=metadata.get('sample_data', [])
                    )
                    metadata['description'] = table_description
                    metadata['columns'] = enriched_columns
                    self.logger.info(f"LLM enrichment completed for {catalog}.{schema}.{table}")
                except Exception as e:
                    self.logger.error(f"LLM enrichment failed: {str(e)}", exc_info=True)
            
            self.metadata_store.store_metadata(
                project_id=project_id,
                connection_id=connection_id,
                catalog=catalog,
                schema=schema,
                table=table,
                metadata=metadata
            )
            return metadata
        except Exception as e:
            self.logger.error(f"Error processing metadata: {str(e)}", exc_info=True)
            raise

    def get_table_metadata_batch(
        self,
        project_id: str,
        connection_id: str,
        tables: List[Dict[str, str]]
    ) -> Dict[str, Any]:
        """Get metadata for multiple tables within a project in batch"""
        results = {
            'successful': [],
            'failed': [],
            'total_processed': 0,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        try:
            connection = self.connection_manager.get_connection(connection_id)
            if not connection:
                raise ValueError(f"No connection found with ID: {connection_id}")
            
            for table_info in tables:
                catalog = table_info['catalog']
                schema = table_info['schema']
                table = table_info['table']
                
                try:
                    self.logger.info(f"Processing table: {catalog}.{schema}.{table}")
                    metadata = self.get_table_metadata(
                        project_id=project_id,
                        connection_id=connection_id,
                        catalog=catalog,
                        schema=schema,
                        table=table
                    )
                    
                    # Explicitly save metadata for each table
                    self.metadata_store.save_metadata(
                        project_id=project_id,
                        connection_id=connection_id,
                        catalog=catalog,
                        schema=schema,
                        table=table,
                        metadata=metadata
                    )
                    
                    results['successful'].append({
                        'catalog': catalog,
                        'schema': schema,
                        'table': table,
                        'metadata': metadata
                    })
                    
                except Exception as e:
                    self.logger.error(f"Failed to process {catalog}.{schema}.{table}: {str(e)}")
                    results['failed'].append({
                        'catalog': catalog,
                        'schema': schema,
                        'table': table,
                        'error': str(e)
                    })
                
                results['total_processed'] += 1
            
            return results
        
        except Exception as e:
            self.logger.error(f"Batch processing failed: {str(e)}")
            raise

    def cleanup_connections(self):
        """Cleanup all active connections"""
        try:
            self.connection_manager.close_all()
        except Exception as e:
            self.logger.error(f"Failed to cleanup connections: {str(e)}")

    def add_data_source(self, project_id: str, connection_id: str, catalog: str, schema: str, tables: Optional[List[str]] = None) -> Dict[str, Any]:
        try:
            connection_details = self.connection_storage.get_connection(connection_id)
            if not connection_details:
                raise ValueError(f"No connection found with ID: {connection_id}")

            connection = TrinoConnectionManager(
                host=connection_details['host'],
                port=connection_details['port'],
                user=connection_details['user'],
                password=connection_details.get('password'),
                http_scheme=connection_details.get('http_scheme', 'http'),
                verify=connection_details.get('verify', True),
                connection_storage=self.connection_storage
            )
            connection.connect()

            if tables is None:
                extractor = GenericMetadataExtractor(connection)
                tables = extractor.get_all_tables(connection, catalog, schema)

            processed_tables = []
            failed_tables = []
            tables_metadata = {}  # Aggregate metadata for tables_metadata.json

            for table in tables:
                try:
                    # Process metadata for individual table (includes column descriptions)
                    metadata = self.process_table_metadata(
                        connection_id=connection_id,
                        project_id=project_id,
                        catalog=catalog,
                        schema=schema,
                        table=table
                    )
                    processed_tables.append(table)

                    # Create a modified copy for tables_metadata.json (no column descriptions)
                    storage_metadata = metadata.copy()
                    for column in storage_metadata['columns']:
                        if 'description' in column:
                            del column['description']
                        if 'sample_values' in column:  # Remove sample_values if present
                            del column['sample_values']
                    tables_metadata[f"{catalog}.{schema}.{table}"] = storage_metadata

                except Exception as e:
                    failed_tables.append({"table": table, "error": str(e)})

            # Update project with data source details
            self.project_manager.add_data_source(
                project_id=project_id,
                connection_id=connection_id,
                catalog=catalog,
                schema=schema,
                tables=tables,
                metadata_store=self.metadata_store
            )

            # Save aggregated metadata to tables_metadata.json without column descriptions
            import os
            import json
            project_dir = os.path.join("storage", "metadata", "projects", project_id)
            tables_metadata_path = os.path.join(project_dir, "tables_metadata.json")
            os.makedirs(project_dir, exist_ok=True)

            # Load existing metadata if it exists
            existing_metadata = {}
            if os.path.exists(tables_metadata_path):
                with open(tables_metadata_path, 'r') as f:
                    existing_metadata = json.load(f)

            # Merge existing metadata with new metadata
            merged_metadata = {**existing_metadata, **tables_metadata}

            # Save merged metadata
            with open(tables_metadata_path, "w") as f:
                json.dump(merged_metadata, f, indent=2)

            return {
                "status": "success",
                "results": {
                    "processed_tables": processed_tables,
                    "failed_tables": failed_tables
                }
            }

        except Exception as e:
            self.logger.error(f"Failed to add data source: {str(e)}")
            raise HTTPException(status_code=400, detail=str(e))

    def handle_type_mismatches(self, table_name: str, error_message: str) -> Dict[str, Any]:
        """Handle known type mismatches during metadata extraction"""
        try:
            if "timestamp" in error_message:
                # For timestamp mismatches, try casting to string
                return self.extract_with_timestamp_cast(table_name)
            elif "varbinary" in error_message:
                # For binary data, exclude those columns
                return self.extract_excluding_binary(table_name)
            else:
                # For other cases, return error metadata
                return {
                    "name": table_name,
                    "error": error_message,
                    "columns": [],
                    "sample_data": [],
                    "extraction_timestamp": datetime.utcnow().isoformat()
                }
        except Exception as e:
            return {
                "name": table_name,
                "error": f"Error handling type mismatch: {str(e)}",
                "columns": [],
                "sample_data": [],
                "extraction_timestamp": datetime.utcnow().isoformat()
            }

    def extract_with_timestamp_cast(self, table_name: str) -> Dict[str, Any]:
        """Extract metadata with explicit timestamp casting for problematic columns"""
        # Implementation would use CAST(timestamp_column AS VARCHAR) in the extraction query
        pass

    def extract_excluding_binary(self, table_name: str) -> Dict[str, Any]:
        """Extract metadata while excluding binary columns"""
        # Implementation would skip or specially handle binary columns
        pass

    def get_tables_for_catalog_schema(
        self,
        connection_id: str,
        catalog: str,
        schema: str
    ) -> List[str]:
        """Get all tables for a given catalog and schema"""
        try:
            connection = self.connection_manager.get_connection(connection_id)
            if not connection:
                raise ValueError(f"No connection found with ID: {connection_id}")
            
            self.logger.info(f"Getting tables for {catalog}.{schema}")
            
            # Create appropriate extractor based on catalog
            extractor = GenericMetadataExtractor(connection)
            appropriate_extractor = extractor.get_appropriate_extractor(catalog)
            
            # Get tables using the appropriate extractor
            tables = appropriate_extractor.get_all_tables(connection, catalog, schema)
            
            if not tables:
                self.logger.warning(f"No tables found in {catalog}.{schema}")
            else:
                self.logger.info(f"Found {len(tables)} tables in {catalog}.{schema}: {tables}")
            
            return tables
        except Exception as e:
            self.logger.error(f"Error getting tables for {catalog}.{schema}: {str(e)}")
            raise

    def process_catalog_metadata(
        self,
        project_id: str,
        connection_id: str,
        catalog: str,
        schema: str
    ) -> Dict[str, Any]:
        """Process metadata for all tables in a catalog/schema"""
        try:
            # Get all tables
            tables = self.get_tables_for_catalog_schema(connection_id, catalog, schema)
            
            results = {
                'successful': [],
                'failed': [],
                'total_processed': 0,
                'timestamp': datetime.utcnow().isoformat()
            }

            for table in tables:
                try:
                    metadata = self.get_table_metadata(
                        project_id=project_id,
                        connection_id=connection_id,
                        catalog=catalog,
                        schema=schema,
                        table=table
                    )
                    
                    results['successful'].append({
                        'catalog': catalog,
                        'schema': schema,
                        'table': table,
                        'metadata': metadata
                    })
                except Exception as e:
                    results['failed'].append({
                        'catalog': catalog,
                        'schema': schema,
                        'table': table,
                        'error': str(e)
                    })
                
                results['total_processed'] += 1

            return results

        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to process catalog metadata: {str(e)}"
            )

    def extract_metadata(self, project_id: str, connection_id: str, catalog: str, schema: str) -> Dict[str, Any]:
        """Extract metadata for all tables in a schema"""
        try:
            connection = self.connection_manager.get_connection(connection_id)
            if not connection:
                raise ValueError(f"Connection not found: {connection_id}")

            extractor = self.get_appropriate_extractor(catalog)
            schema_metadata = extractor.extract_schema_metadata(connection, catalog, schema)
            
            # Save metadata for each table
            for table_name, table_metadata in schema_metadata.items():
                self.metadata_store.save_metadata(
                    project_id=project_id,
                    connection_id=connection_id,
                    catalog=catalog,
                    schema=schema,
                    table=table_name,
                    metadata=table_metadata
                )
            
            return schema_metadata
        except Exception as e:
            logging.error(f"Error extracting metadata: {str(e)}")
            raise

    def get_table_metadata(
        self,
        connection_id: str,
        project_id: str,
        catalog: str,
        schema: str,
        table: str
    ) -> Dict[str, Any]:
        """Get metadata for a specific table"""
        try:
            # Get connection details
            connection_details = self.connection_storage.get_connection(connection_id)
            if not connection_details:
                raise ValueError(f"No connection found with ID: {connection_id}")

            # Create new connection
            connection = TrinoConnectionManager(
                host=connection_details['host'],
                port=connection_details['port'],
                user=connection_details['user'],
                password=connection_details.get('password'),
                http_scheme=connection_details.get('http_scheme', 'http'),
                verify=connection_details.get('verify', True)
            )
            connection.connect()

            # Extract metadata
            extractor = GenericMetadataExtractor(connection)
            metadata = extractor.extract_table_metadata(connection, catalog, schema, table)

            # Store metadata
            self.metadata_store.store_metadata(
                project_id=project_id,
                connection_id=connection_id,
                catalog=catalog,
                schema=schema,
                table=table,
                metadata=metadata
            )

            return metadata

        except Exception as e:
            self.logger.error(f"Failed to get metadata for {catalog}.{schema}.{table}: {str(e)}")
            raise

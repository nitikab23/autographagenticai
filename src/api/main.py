from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from pydantic import BaseModel
from typing import List, Dict, Optional

from src.core.service_layer import ApplicationService
from src.metadata_management.metadata_store import MetadataStore  # Add if needed

# Application state
class AppState:
    def __init__(self):
        self.service = None

app_state = AppState()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    app_state.service = ApplicationService()
    yield
    # Shutdown
    app_state.service.cleanup_connections()

app = FastAPI(
    title="AutoAI Platform",
    description="AI-driven data platform for metadata extraction and querying",
    version="0.1.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dependency for service access
async def get_service():
    return app_state.service

# Trino Connection Endpoints
@app.post("/connections/trino")
async def create_trino_connection(
    connection_details: Dict,
    service: ApplicationService = Depends(get_service)
):
    """Create a new Trino connection"""
    return service.connect_trino(connection_details)

@app.get("/connections/trino")
def list_trino_connections(
    service: ApplicationService = Depends(get_service)
):
    """
    List all saved Trino connections
    """
    return service.list_trino_connections()

@app.get("/connections/trino/{connection_id}")
def get_trino_connection_details(
    connection_id: str,
    service: ApplicationService = Depends(get_service)
):
    """
    Get details for a specific Trino connection
    """
    return service.get_trino_connection_details(connection_id)

@app.get("/connections/trino/{connection_id}/catalogs")
def get_trino_connection_catalogs(
    connection_id: str,
    service: ApplicationService = Depends(get_service)
):
    """
    Get available catalogs for a Trino connection
    """
    return service.get_trino_connection_details(connection_id)

@app.get("/connections/trino/{connection_id}/catalogs/{catalog}/schemas")
def get_trino_connection_schemas(
    connection_id: str,
    catalog: str,
    service: ApplicationService = Depends(get_service)
):
    """
    Get available schemas for a specific catalog in a Trino connection
    """
    return service.get_trino_connection_schemas(connection_id, catalog)

@app.get("/connections/trino/{connection_id}/catalogs/{catalog}/schemas/{schema}/tables")
def get_trino_connection_tables(
    connection_id: str, 
    catalog: str, 
    schema: str,
    service: ApplicationService = Depends(get_service)
):
    """
    Get available tables for a specific schema in a Trino connection
    """
    return service.get_trino_connection_tables(connection_id, catalog, schema)

# Project Management Endpoints
@app.post("/projects")
def create_project(
    name: str, 
    description: str = '',
    service: ApplicationService = Depends(get_service)
):
    """
    Create a new project
    """
    return service.create_project(name, description)

@app.get("/projects")
def list_projects(
    service: ApplicationService = Depends(get_service)
):
    """
    List all projects
    """
    return service.list_projects()

@app.get("/projects/{project_id}")
def get_project(
    project_id: str,
    service: ApplicationService = Depends(get_service)
):
    """
    Get details of a specific project
    """
    return service.get_project(project_id)

@app.post("/projects/{project_id}/data-sources")
def add_data_source(
    project_id: str,
    connection_id: str,
    catalog: str,
    schema: str,
    tables: Optional[List[str]] = None,
    service: ApplicationService = Depends(get_service)
):
    """
    Add a data source to a project and extract its metadata
    
    If tables is None, all available tables in the schema will be discovered and processed
    """
    return service.add_data_source(
        project_id=project_id,
        connection_id=connection_id,
        catalog=catalog,
        schema=schema,
        tables=tables
    )

@app.delete("/projects/{project_id}")
def delete_project(
    project_id: str,
    service: ApplicationService = Depends(get_service)
):
    """
    Delete a specific project and its associated resources
    """
    return service.delete_project(project_id)

@app.get("/projects/{project_id}/connections/{connection_id}/metadata/{catalog}/{schema}/{table}")
def get_table_metadata(
    project_id: str,
    connection_id: str,
    catalog: str,
    schema: str,
    table: str,
    service: ApplicationService = Depends(get_service)
):
    """
    Get metadata for a specific table within a project
    """
    return service.get_table_metadata(
        project_id=project_id,
        connection_id=connection_id,
        catalog=catalog,
        schema=schema,
        table=table
    )

@app.post("/projects/{project_id}/connections/{connection_id}/metadata/batch")
def get_table_metadata_batch(
    project_id: str,
    connection_id: str,
    tables: List[Dict[str, str]],
    service: ApplicationService = Depends(get_service)
):
    """
    Get metadata for multiple tables within a project in batch
    """
    return service.get_table_metadata_batch(
        project_id=project_id,
        connection_id=connection_id,
        tables=tables
    )

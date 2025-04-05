from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from pydantic import BaseModel
from typing import List, Dict, Optional
import os # Import os
import pandas as pd # Import pandas

from src.core.service_layer import ApplicationService
from src.metadata_management.metadata_store import MetadataStore  # Add if needed
from src.agents.context import ContextProtocol
from src.agents.coordinator import CoordinatorAgent

# Add new Pydantic model for query requests
class QueryRequest(BaseModel):
    query: str
    project_id: str
    clarifications: Optional[Dict[str, str]] = None  # Simple key-value store for clarifications

# Application state
class AppState:
    def __init__(self):
        self.service = None

app_state = AppState()

# Setup templates directory - Ensure this path is correct relative to where main.py is run
# Since main.py is in src/api, templates are in ./templates
templates_dir = os.path.join(os.path.dirname(__file__), "templates")
templates = Jinja2Templates(directory=templates_dir)


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

# --- Frontend Endpoint ---
@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """Serves the main index.html file"""
    return templates.TemplateResponse("index.html", {"request": request})

# --- API Endpoints ---

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

@app.post("/analyze")
async def analyze_query(
    request: QueryRequest,
    service: ApplicationService = Depends(get_service)
):
    """Analyze a query using metadata agents"""
    try:
        # Verify project exists
        project = service.get_project(request.project_id)
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")

        # Create context with project ID
        # Clarifications are handled internally or via updates, not in __init__
        context = ContextProtocol(
            query=request.query,
            project_id=request.project_id
            # Removed clarifications=request.clarifications
        )

        # --- Get Connection Manager for the Project ---
        # Assuming project details contain data source info including connection_id
        # This logic might need adjustment based on actual project structure
        connection_id = None
        if isinstance(project, dict) and project.get('data_sources'):
             # Assuming the first data source is the relevant one for now
             connection_id = project['data_sources'][0].get('connection_id')

        if not connection_id:
             # Handle case where project has no connection_id or structure is different
             raise HTTPException(status_code=400, detail=f"Could not determine connection ID for project {request.project_id}")

        try:
            # Get the specific connection manager instance for this project/connection
            conn_manager = service.get_connection(connection_id)
        except Exception as conn_err:
             raise HTTPException(status_code=500, detail=f"Failed to get connection manager for ID {connection_id}: {conn_err}")

        # If the request includes clarifications (i.e., user is responding), update the context
        if request.clarifications:
            context = context.update({"clarifications": request.clarifications})
            # Log or print for debugging if needed
            # print(f"Updated context with clarifications: {request.clarifications}")

        # Initialize and execute coordinator agent, passing the connection manager
        coordinator = CoordinatorAgent(context, conn_manager) # Pass conn_manager
        result = await coordinator.execute()

        # Check if clarification is needed
        if result.operation == "clarification_needed":
            return {
                "status": "clarification_needed",
                "clarifications": result.details.get("ambiguities", []), # Use .get for safety
                "reasoning_steps": context.get("reasoning_steps", []), # Include steps so far
                "sql_query": context.get("sql_query"),
                "data_summary": None, # No data yet
                "visualization_html": None,
                "error_message": None
            }

        # If successful completion (or ended after visualization)
        # --- Get the FINAL context state AFTER execution ---
        final_context = coordinator.context.snapshot() # Get snapshot from the coordinator instance
        reasoning_steps = final_context.get("reasoning_steps", [])

        # --- Extract visualization HTML and summary by finding the correct step ---
        visualization_html = None
        visualization_summary = None
        # Iterate backwards through steps to find the visualization result
        for step in reversed(reasoning_steps):
             # Ensure step and its details are dictionaries
             if isinstance(step, dict):
                 agent = step.get("agent")
                 action = step.get("action")
                 if agent == "VisualizationAgent" and action == "visualization_generated":
                     details = step.get("details", {})
                     if isinstance(details, dict):
                         visualization_html = details.get("visualization_html")
                         visualization_summary = details.get("visualization_summary")
                         break # Stop searching once found

        # --- Generate HTML Table from CSV ---
        data_table_html = None
        query_result_path = final_context.get("query_result_path")
        if query_result_path and os.path.exists(query_result_path):
            try:
                # Read limited rows for display
                df_sample = pd.read_csv(query_result_path, nrows=20)
                # Convert to HTML table with some basic styling (can be enhanced)
                data_table_html = df_sample.to_html(index=False, classes="min-w-full divide-y divide-gray-200 text-xs", border=0)
                # Replace default table style with Tailwind classes if needed, e.g.,
                # data_table_html = data_table_html.replace('<table border="1" class="dataframe">', '<table class="min-w-full divide-y divide-gray-200 text-xs">')
                # data_table_html = data_table_html.replace('<thead>', '<thead class="bg-gray-50">')
                # data_table_html = data_table_html.replace('<th>', '<th class="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">')
                # data_table_html = data_table_html.replace('<tbody>', '<tbody class="bg-white divide-y divide-gray-200">')
                # data_table_html = data_table_html.replace('<td>', '<td class="px-3 py-2 whitespace-nowrap">')
            except Exception as read_err:
                # Log error reading file, but don't fail the whole request
                print(f"Warning: Could not read or convert CSV {query_result_path} to HTML table: {read_err}")


        return {
            "status": "success", # Assuming completion means success for now
            "reasoning_steps": reasoning_steps,
            "sql_query": final_context.get("sql_query"),
            "data_summary": f"{final_context.get('query_result_row_count', 'N/A')} rows retrieved", # Example summary
            "data_table_html": data_table_html, # Add HTML table
            "visualization_html": visualization_html,
            "visualization_summary": visualization_summary, # Add summary to response
            "assumptions": final_context.get("assumptions", []), # Add assumptions list
            "clarifications": None, # Keep this null as we removed the clarification step
            "error_message": None
        }
    except Exception as e:
        # Log the exception details if possible
        # logger.error(f"Error during analysis: {e}", exc_info=True) # Assuming logger is available
        return {
            "status": "error",
            "reasoning_steps": context.get("reasoning_steps", []) if 'context' in locals() else [], # Include steps if context exists
            "sql_query": context.get("sql_query") if 'context' in locals() else None,
            "data_summary": None,
            "data_table_html": None, # Add table field to error response
            "visualization_html": None,
            "visualization_summary": None, # Add summary field to error response
            "assumptions": None, # Add assumptions field to error response
            "clarifications": None,
            "error_message": str(e)
        }
        # Consider re-raising HTTPException for specific known errors vs. returning JSON for all
        # raise HTTPException(status_code=500, detail=str(e))

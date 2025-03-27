import os
import sys
import socket
import requests
from urllib3.exceptions import InsecureRequestWarning
from dotenv import load_dotenv

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
load_dotenv()

from src.core.service_layer import ApplicationService
from src.metadata_management.llm_enricher import MetadataEnricher
from src.metadata_management.metadata_store import MetadataStore
import logging
import time
from typing import Dict, List, Any
from functools import partial
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def retry_operation(operation, max_attempts=3, delay=1):
    """Retry an operation with exponential backoff"""
    for attempt in range(max_attempts):
        try:
            return operation()
        except Exception as e:
            if attempt == max_attempts - 1:
                raise
            time.sleep(delay * (2 ** attempt))

def process_table_metadata(
    service: ApplicationService,
    connection_id: str,
    project_id: str,
    catalog: str,
    schema: str,
    table: str
) -> Dict[str, Any]:
    """Process metadata for a single table"""
    table_identifier = f"{catalog}.{schema}.{table}"
    print(f"\nProcessing table: {table_identifier}")
    
    try:
        metadata = service.get_table_metadata(
            connection_id=connection_id,
            project_id=project_id,
            catalog=catalog,
            schema=schema,
            table=table
        )
        return metadata
    except Exception as e:
        print(f"Failed to process {table_identifier}: {str(e)}")
        raise

def check_trino_availability(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=5) as sock:
            print(f"Successfully connected to {host}:{port}")
            return True
    except (socket.timeout, socket.error) as e:
        print(f"Failed to connect to {host}:{port}: {str(e)}")
        return False

def verify_trino_http(host: str, port: int, user: str, use_https: bool = False) -> bool:
    protocol = "https" if use_https else "http"
    url = f"{protocol}://{host}:{port}/v1/info"
    headers = {"X-Trino-User": user}
    try:
        response = requests.get(url, headers=headers, timeout=5, verify=False)
        if response.status_code == 200:
            print(f"Trino {protocol.upper()} endpoint is accessible: {url}")
            return True
        return False
    except requests.exceptions.RequestException as e:
        print(f"Failed to connect to Trino {protocol.upper()} endpoint: {str(e)}")
        return False

def setup_trino_connection(service):
    connection_details = {
        'host': os.getenv('TRINO_HOST', 'localhost'),
        'port': int(os.getenv('TRINO_PORT', '8080')),
        'user': os.getenv('TRINO_USERNAME', 'trino'),  # Store as 'user'
        'password': os.getenv('TRINO_PASSWORD', ''),
        'http_scheme': os.getenv('TRINO_HTTP_SCHEME', 'http'),
        'verify': os.getenv('TRINO_VERIFY_SSL', 'false').lower() == 'true'
    }
    print("\nVerifying Trino server availability...")
    print("\nConnection details:", {k: v for k, v in connection_details.items() if k != 'password'})
    
    if not check_trino_availability(connection_details['host'], connection_details['port']):
        return None
        
    http_schemes = ['http', 'https'] if connection_details['http_scheme'] == 'https' else ['http']
    success = False
    
    for scheme in http_schemes:
        if verify_trino_http(connection_details['host'], connection_details['port'], 
                           connection_details['user'], use_https=(scheme == 'https')):
            connection_details['http_scheme'] = scheme
            success = True
            break
            
    if not success:
        return None
        
    print("\nSetting up Trino connection...")
    try:
        result = service.connect_trino(connection_details)
        print(f"\nConnection created successfully! ID: {result['connection_id']}")
        return result['connection_id']
    except Exception as e:
        print(f"\nFailed to create connection: {str(e)}")
        return None

def test_metadata_creation():
    logging.basicConfig(level=logging.DEBUG)  # Change to DEBUG level
    logger = logging.getLogger(__name__)
    
    # Verify OpenAI API key
    openai_api_key = os.getenv('OPENAI_API_KEY')
    if not openai_api_key:
        logger.error("OPENAI_API_KEY environment variable is not set")
        return
    logger.info("OpenAI API key is set")
    
    service = ApplicationService()
    if service.metadata_enricher is None:
        logger.error("LLM enricher not initialized. Check if OPENAI_API_KEY is set.")
        return
        
    logger.info("LLM enrichment is enabled")
    
    try:
        # Create test project with explicit skip_llm=False
        print("\nCreating test project...")
        project = service.create_project(
            name="Test Project",
            description="Test project for metadata creation",
            skip_llm=False  # Explicitly set to False
        )
        
        # 2. Setup Trino connection
        print("\nSetting up Trino connection...")
        connection_id = setup_trino_connection(service)
        if not connection_id:
            raise Exception("Failed to establish Trino connection")
        
        # 3. Add multiple data sources to project
        data_sources = [
            {'catalog': 'hive', 'schema': 'rentals_db', 'tables': None},
            {'catalog': 'postgresql', 'schema': 'public', 'tables': None}
        ]
        
        for source in data_sources:
            print(f"\n{'='*80}")
            print(f"Processing data source: {source['catalog']}.{source['schema']}")
            print(f"{'='*80}")
            
            try:
                # First, try to list available tables
                tables = service.get_trino_connection_tables(
                    connection_id=connection_id,
                    catalog=source['catalog'],
                    schema=source['schema']
                )
                print(f"\nAvailable tables in {source['catalog']}.{source['schema']}:")
                print(tables.get('available_tables', []))
                
                # Then process the data source
                result = service.add_data_source(
                    project_id=project['id'],
                    connection_id=connection_id,
                    catalog=source['catalog'],
                    schema=source['schema'],
                    tables=source['tables']
                )
                
                if result['status'] != 'success':
                    print(f"\nFailed to add data source: {result.get('error')}")
                    continue
                    
                print(f"\nProcessed tables: {result['results']['processed_tables']}")
                if result['results']['failed_tables']:
                    print("\nFailed tables:")
                    for failed in result['results']['failed_tables']:
                        print(f"- {failed['table']}: {failed['error']}")
                        
            except Exception as e:
                print(f"\nError processing {source['catalog']}.{source['schema']}: {str(e)}")
            
    finally:
        print("\nCleaning up connections...")
        service.cleanup_connections()

if __name__ == "__main__":
    print("Starting metadata creation test with LLM enrichment...")
    test_metadata_creation()  # skip_llm is already False by default

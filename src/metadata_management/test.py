import os
import logging
from src.core.service_layer import ApplicationService
from src.metadata_management.llm_enricher import MetadataEnricher

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def test_metadata_creation():
    service = ApplicationService()
    
    if service.metadata_enricher is None:
        print("LLM enricher not initialized. Check OPENAI_API_KEY.")
        return
    
    print("LLM enrichment is enabled")
    
    project = service.create_project(
        name="Test Project",
        description="Test project for metadata creation",
        skip_llm=False
    )
    print(f"Created project: {project['id']}")

    connection_details = {
        'host': 'localhost',
        'port': 8080,
        'user': 'trino',
        'http_scheme': 'http',
        'verify': False
    }
    connection_result = service.connect_trino(connection_details)
    connection_id = connection_result['connection_id']
    print(f"Connection ID: {connection_id}")

    result = service.add_data_source(
        project_id=project['id'],
        connection_id=connection_id,
        catalog='hive',
        schema='rentals_db',
        tables=['address']
    )
    print(f"Data source result: {result}")

if __name__ == "__main__":
    test_metadata_creation()
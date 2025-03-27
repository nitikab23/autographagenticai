from typing import List, Dict, Optional
from src.trino_connection.connection_manager import TrinoConnectionManager
from src.metadata_management.metadata_extractor import MetadataManager  # Add if needed

class CatalogBrowser:
    """
    Provides an interactive and comprehensive way to browse 
    and select databases from a Trino engine
    """
    def __init__(self, connection_manager: TrinoConnectionManager):
        """
        Initialize Catalog Browser
        
        Args:
            connection_manager (TrinoConnectionManager): Active Trino connection
        """
        self.connection_manager = connection_manager
    
    def discover_available_databases(self) -> Dict[str, Dict[str, List[str]]]:
        """
        Comprehensive database discovery across catalogs
        
        Returns:
            Nested dictionary of catalogs, schemas, and tables
        """
        return self.connection_manager.get_catalog_structure()
    
    def interactive_database_selection(self) -> List[Dict]:
        """
        Interactive method for users to select databases
        
        Returns:
            List of selected database configurations
        """
        available_databases = self.discover_available_databases()
        selected_databases = []
        
        for catalog, schemas in available_databases.items():
            print(f"\n📁 Catalog: {catalog}")
            
            for schema, tables in schemas.items():
                print(f"  📂 Schema: {schema}")
                
                # Display tables
                for i, table in enumerate(tables, 1):
                    print(f"    {i}. {table}")
                
                # Prompt for table selection
                selection = input(
                    f"Select tables from {catalog}.{schema} "
                    "(comma-separated numbers, or press Enter to skip): "
                ).strip()
                
                if selection:
                    try:
                        selected_table_indices = [
                            int(idx.strip()) - 1 
                            for idx in selection.split(',')
                        ]
                        
                        selected_tables = [
                            tables[idx] for idx in selected_table_indices 
                            if 0 <= idx < len(tables)
                        ]
                        
                        if selected_tables:
                            selected_databases.append({
                                'catalog': catalog,
                                'schema': schema,
                                'tables': selected_tables
                            })
                    
                    except (ValueError, IndexError) as e:
                        print(f"Invalid selection. Skipping {catalog}.{schema}")
        
        return selected_databases
    
    def get_table_details(self, catalog: str, schema: str, table: str) -> Dict:
        """
        Retrieve detailed information about a specific table
        
        Args:
            catalog (str): Catalog name
            schema (str): Schema name
            table (str): Table name
        
        Returns:
            Dict: Table metadata and structure
        """
        # Retrieve table metadata using connection manager
        table_metadata = self.connection_manager.get_table_metadata(catalog, schema, table)
        
        return {
            'catalog': catalog,
            'schema': schema,
            'table': table,
            'metadata': table_metadata
        }

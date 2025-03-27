import logging
import re
from typing import List, Dict, Optional, Any
from abc import ABC, abstractmethod
from datetime import datetime, date
from decimal import Decimal
import json
from src.trino_connection.connection_manager import TrinoConnectionManager
from .metadata_store import MetadataStore
from .models import TableMetadata

logger = logging.getLogger(__name__)

def _convert_value(value):
    """Convert special data types to JSON-serializable format"""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value

class DatabaseMetadataExtractor(ABC):
    """Abstract base class for database-specific metadata extraction"""
    
    def __init__(self):
        self._connection = None

    def set_connection(self, connection):
        """Set the connection for the extractor"""
        self._connection = connection

    @abstractmethod
    def get_all_tables(self, connection, catalog: str, schema: str) -> List[str]:
        """Get all tables in the specified catalog and schema - must be implemented by subclasses"""
        pass

    @abstractmethod
    def extract_table_metadata(self, connection, catalog: str, schema: str, table: str) -> Dict[str, Any]:
        """Extract metadata for a specific table - must be implemented by subclasses"""
        pass

class PostgreSQLMetadataExtractor(DatabaseMetadataExtractor):
    """PostgreSQL-specific metadata extraction"""
    
    def __init__(self):
        super().__init__()

    def get_all_tables(self, connection, catalog: str, schema: str) -> List[str]:
        """Get all tables in the specified schema"""
        try:
            cursor = connection.cursor()
            # Modified query to work with Trino's PostgreSQL catalog
            query = f"""
                SELECT table_name 
                FROM {catalog}.information_schema.tables 
                WHERE table_schema = '{schema}'
                AND table_type IN ('BASE TABLE', 'VIEW')
            """
            
            cursor.execute(query)
            tables = [row[0] for row in cursor.fetchall()]
            return tables
        except Exception as e:
            self.logger.error(f"Error getting PostgreSQL tables for {schema}: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()

    def extract_table_metadata(self, connection, catalog: str, schema: str, table: str) -> Dict[str, Any]:
        """Extract metadata for a specific table"""
        cursor = None
        try:
            cursor = connection.cursor()
            
            # Get column information using Trino's PostgreSQL syntax
            columns_query = f"""
                SELECT 
                    column_name,
                    data_type,
                    CASE WHEN is_nullable = 'YES' THEN true ELSE false END as is_nullable
                FROM {catalog}.information_schema.columns
                WHERE table_schema = '{schema}'
                AND table_name = '{table}'
                ORDER BY ordinal_position
            """
            
            cursor.execute(columns_query)
            columns = cursor.fetchall()
            
            # Get sample data
            sample_query = f"SELECT * FROM {catalog}.{schema}.{table} LIMIT 5"
            cursor.execute(sample_query)
            sample_data = []
            column_names = []
            
            if cursor.description:
                column_names = [desc[0] for desc in cursor.description]
                sample_data = [
                    {col: _convert_value(val) if val is not None else None 
                     for col, val in zip(column_names, row)}
                    for row in cursor.fetchall()
                ]

            # Create column metadata with sample values
            columns_with_samples = [
                {
                    'name': col[0],
                    'type': col[1],
                    'nullable': col[2],
                    'description': '',
                    'sample_values': [
                        str(row.get(col[0])) 
                        for row in sample_data[:3]
                        if row.get(col[0]) is not None
                    ]
                }
                for col in columns
            ]

            metadata = {
                'name': table,
                'schema': schema,
                'catalog': catalog,
                'columns': columns_with_samples,
                'sample_data': sample_data,
                'extraction_timestamp': datetime.now().isoformat(),
                'last_updated': datetime.now().isoformat()
            }
            
            return metadata
            
        except Exception as e:
            self.logger.error(f"Failed to extract metadata for {catalog}.{schema}.{table}: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()

    def _get_table_type(self, cursor, schema: str, table: str) -> str:
        """Determine if table is base table or view"""
        try:
            cursor.execute("""
                SELECT table_type 
                FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name = %s
            """, (schema, table))
            result = cursor.fetchone()
            return result[0] if result else 'UNKNOWN'
        except Exception:
            return 'UNKNOWN'

    def _get_sample_data(self, cursor, schema: str, table: str, limit: int = 5) -> List[Dict]:
        """Get sample rows from table"""
        try:
            cursor.execute(f"SELECT * FROM {schema}.{table} LIMIT {limit}")
            if not cursor.description:
                return []
            
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception as e:
            logger.warning(f"Failed to get sample data for {schema}.{table}: {str(e)}")
            return []

    def _get_row_count(self, cursor, schema: str, table: str) -> Optional[int]:
        """Get approximate row count"""
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
            return cursor.fetchone()[0]
        except Exception as e:
            logger.warning(f"Failed to get row count for {schema}.{table}: {e}")
            return None

class MySQLMetadataExtractor(DatabaseMetadataExtractor):
    """MySQL-specific metadata extractor"""
    
    def __init__(self, connection):
        super().__init__(connection)

    def get_primary_keys(self, cursor, schema: str, table: str) -> List[str]:
        """Get primary keys for MySQL table"""
        query = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = %s
        AND TABLE_NAME = %s
        AND CONSTRAINT_NAME = 'PRIMARY'
        ORDER BY ORDINAL_POSITION;
        """
        cursor.execute(query, (schema, table))
        return [row[0] for row in cursor.fetchall()]

    def get_foreign_keys(self, cursor, schema: str, table: str) -> List[Dict]:
        """Get foreign keys for MySQL table"""
        query = """
        SELECT
            COLUMN_NAME,
            REFERENCED_TABLE_NAME,
            REFERENCED_TABLE_SCHEMA,
            REFERENCED_COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = %s
        AND TABLE_NAME = %s
        AND REFERENCED_TABLE_NAME IS NOT NULL;
        """
        cursor.execute(query, (schema, table))
        return [{
            'column': row[0],
            'referenced_table': row[1],
            'referenced_schema': row[2],
            'referenced_column': row[3]
        } for row in cursor.fetchall()]

    def get_columns(self, cursor, schema: str, table: str) -> List[Dict]:
        """Get columns for MySQL table"""
        query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
        AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION;
        """
        cursor.execute(query, (schema, table))
        return [{
            'name': row[0],
            'type': row[1],
            'nullable': row[2] == 'YES',
            'default': row[3],
            'max_length': row[4],
            'numeric_precision': row[5],
            'numeric_scale': row[6]
        } for row in cursor.fetchall()]

class TrinoMetadataExtractor(DatabaseMetadataExtractor):
    """Trino-specific metadata extractor"""
    
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__name__)

    def get_all_tables(self, connection, catalog: str, schema: str) -> List[str]:
        """Get all tables in the specified catalog and schema"""
        try:
            cursor = connection.cursor()
            query = f"""
                SELECT table_name 
                FROM {catalog}.information_schema.tables 
                WHERE table_schema = '{schema}'
                AND table_type IN ('BASE TABLE', 'VIEW')
                ORDER BY table_name
            """
            
            self.logger.info(f"Executing query: {query}")
            cursor.execute(query)
            tables = [row[0] for row in cursor.fetchall()]
            self.logger.info(f"Found {len(tables)} tables in {catalog}.{schema}")
            return tables
        except Exception as e:
            self.logger.error(f"Error getting tables for {catalog}.{schema}: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()

    def _serialize_value(self, value):
        """Safely serialize values for JSON"""
        if value is None:
            return None
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, bytes):  # Handle varbinary/blob data
            return "[BINARY DATA]"
        return str(value)

    def extract_table_metadata(self, connection, catalog: str, schema: str, table: str) -> Dict[str, Any]:
        """Extract metadata for a specific table"""
        cursor = None
        try:
            cursor = connection.cursor()
            
            # Get column information
            columns_query = f"""
                SELECT column_name, data_type, is_nullable
                FROM {catalog}.information_schema.columns
                WHERE table_schema = '{schema}'
                AND table_name = '{table}'
                ORDER BY ordinal_position
            """
            
            cursor.execute(columns_query)
            columns = cursor.fetchall()
            
            # Get sample data with safe type handling
            try:
                # First try with a LIMIT 0 to get column types safely
                safe_query = f"SELECT * FROM {catalog}.{schema}.{table} LIMIT 0"
                cursor.execute(safe_query)
                column_types = {desc[0]: desc[1] for desc in cursor.description} if cursor.description else {}
                
                # Then try to get actual sample data
                sample_data = []
                sample_query = f"SELECT * FROM {catalog}.{schema}.{table} LIMIT 3"
                try:
                    cursor.execute(sample_query)
                    sample_data = [
                        {
                            col[0]: self._serialize_value(val)
                            for col, val in zip(cursor.description, row)
                        }
                        for row in cursor.fetchall()
                    ]
                except Exception as e:
                    self.logger.warning(f"Failed to get sample data: {str(e)}")
                    # Provide empty sample if we can't get real data
                    sample_data = [
                        {col[0]: None for col in columns}
                        for _ in range(3)
                    ]
            
            except Exception as e:
                self.logger.warning(f"Failed to get sample data: {str(e)}")
                sample_data = []
            
            # Get row count with error handling
            row_count = None
            try:
                count_query = f"SELECT COUNT(*) FROM {catalog}.{schema}.{table}"
                cursor.execute(count_query)
                row_count = cursor.fetchone()[0]
            except Exception as e:
                self.logger.warning(f"Failed to get row count: {str(e)}")

            metadata = {
                'name': table,
                'schema': schema,
                'catalog': catalog,
                'columns': [
                    {
                        'name': col[0],
                        'type': col[1],
                        'nullable': col[2] == 'YES',
                        'description': '',
                        'sample_values': [
                            str(data.get(col[0])) for data in sample_data[:3]
                            if data.get(col[0]) is not None
                        ]
                    }
                    for col in columns
                ],
                'sample_data': sample_data,
                'row_count': row_count,
                'description': '',
                'extraction_timestamp': datetime.now().isoformat(),
                'last_updated': datetime.now().isoformat()
            }
            
            self.logger.debug(f"Extracted metadata for {catalog}.{schema}.{table}")
            return metadata
        
        except Exception as e:
            self.logger.error(f"Failed to extract metadata for {catalog}.{schema}.{table}: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()

class GenericMetadataExtractor:
    """Factory class for creating appropriate metadata extractors"""
    
    def __init__(self, connection):
        self.connection = connection
        self.logger = logging.getLogger(__name__)
        self.extractors = {
            'postgresql': PostgreSQLMetadataExtractor(),
            'trino': TrinoMetadataExtractor(),
            'hive': TrinoMetadataExtractor()  # Hive is accessed through Trino
        }
        
        # Initialize extractors with connection
        for extractor in self.extractors.values():
            extractor.set_connection(connection)
    
    def get_appropriate_extractor(self, catalog: str) -> DatabaseMetadataExtractor:
        """Get the appropriate extractor based on catalog type"""
        catalog_lower = catalog.lower()
        
        # Default to Trino extractor for unknown catalogs
        extractor = self.extractors.get(catalog_lower, self.extractors['trino'])
        self.logger.info(f"Using {extractor.__class__.__name__} for catalog {catalog}")
        
        # Ensure connection is set
        extractor.set_connection(self.connection)
        return extractor

    def get_all_tables(self, connection, catalog: str, schema: str) -> List[str]:
        """Get all tables using the appropriate extractor"""
        extractor = self.get_appropriate_extractor(catalog)
        return extractor.get_all_tables(connection, catalog, schema)

    def extract_table_metadata(self, connection, catalog: str, schema: str, table: str) -> Dict[str, Any]:
        """Extract metadata using the appropriate extractor"""
        extractor = self.get_appropriate_extractor(catalog)
        return extractor.extract_table_metadata(connection, catalog, schema, table)

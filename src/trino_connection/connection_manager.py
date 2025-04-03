from datetime import datetime, date
from decimal import Decimal
import trino
import logging
import csv # Import csv module
import os # Import os for path operations
from typing import List, Dict, Optional, Any
from .connection_config import SecureConnectionStorage

logger = logging.getLogger(__name__)

class TrinoConnectionManager:
    """Manages connections to Trino server"""

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: Optional[str] = None,
        http_scheme: str = 'http',
        verify: bool = True,
        connection_storage: Optional[SecureConnectionStorage] = None
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.http_scheme = http_scheme
        self.verify = verify
        self._connection = None
        self._sqlalchemy_engine = None
        self.current_catalog = None
        self.current_schema = None
        self.connection_storage = connection_storage

    def connect(self) -> 'TrinoConnectionManager':
        """Establish connection to Trino"""
        # Avoid reconnecting if already connected
        if self._connection:
            # TODO: Add check if connection is still valid?
            return self

        try:
            import trino
            connection_params = {
                'host': self.host,
                'port': self.port,
                'user': self.user,
                'http_scheme': self.http_scheme,
                'verify': self.verify
            }

            if self.password:
                connection_params['auth'] = trino.auth.BasicAuthentication(
                    self.user,
                    self.password
                )

            self._connection = trino.dbapi.connect(**connection_params)
            logger.info(f"Successfully connected to Trino at {self.host}:{self.port}")
            return self
        except Exception as e:
            logger.error(f"Failed to connect to Trino: {str(e)}")
            raise

    # Removed duplicate get_connection method

    @property
    def connection(self):
        """Get the current connection"""
        if not self._connection:
             self.connect() # Attempt to connect if not already connected
        return self._connection

    def cursor(self):
        """Get a cursor from the connection"""
        return self.connection.cursor() # Use property to ensure connection

    @classmethod
    def from_connection_details(cls, details: Dict[str, Any]) -> 'TrinoConnectionManager':
        """Create connection manager from connection details dictionary"""
        return cls(
            host=details['host'],
            port=details['port'],
            user=details['user'],
            password=details.get('password'),
            http_scheme=details.get('http_scheme', 'http'),
            verify=details.get('verify', True)
        )

    def connect_to_catalog(self, catalog: str, schema: str = None) -> 'TrinoConnectionManager':
        """Connect to specific catalog and schema"""
        try:
            # If already connected to this catalog/schema, no need to reconnect
            if (self.current_catalog == catalog and
                self.current_schema == schema and
                self._connection is not None):
                return self

            # Close existing connection if any
            if self._connection:
                self.close()

            # Create new connection with catalog/schema
            import trino
            connection_params = {
                'host': self.host,
                'port': self.port,
                'user': self.user,
                'catalog': catalog,  # Explicitly set catalog
                'http_scheme': self.http_scheme,
                'verify': self.verify
            }

            # Only add schema if it's provided
            if schema:
                connection_params['schema'] = schema

            if self.password:
                connection_params['auth'] = trino.auth.BasicAuthentication(
                    self.user,
                    self.password
                )

            self._connection = trino.dbapi.connect(**connection_params)

            if not self._connection:
                raise Exception(f"Failed to establish connection to {catalog}.{schema}")

            self.current_catalog = catalog
            self.current_schema = schema
            logger.info(f"Connected to Trino catalog: {catalog}, schema: {schema}")
            return self

        except Exception as e:
            logger.error(f"Failed to connect to catalog {catalog}: {str(e)}")
            raise

    def get_tables(self, catalog: str, schema: str) -> List[str]:
        """Get list of tables in the specified catalog and schema"""
        # First ensure we're connected to the right catalog/schema
        self.connect_to_catalog(catalog, schema)

        query = f"""
        SELECT table_name
        FROM {catalog}.information_schema.tables
        WHERE table_catalog = '{catalog}'
        AND table_schema = '{schema}'
        """

        cursor = None
        try:
            cursor = self.cursor()
            cursor.execute(query)
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get tables for {catalog}.{schema}: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()

    def get_table_info(self, catalog: str, schema: str, table: str) -> Dict[str, Any]:
        """Get detailed table information including columns and sample data"""
        # Ensure connection to the correct catalog/schema
        self.connect_to_catalog(catalog, schema)
        cursor = None
        try:
            cursor = self.cursor()

            # Get column information
            cursor.execute(f"""
                SELECT column_name, data_type, is_nullable
                FROM {catalog}.information_schema.columns
                WHERE table_catalog = '{catalog}'
                AND table_schema = '{schema}'
                AND table_name = '{table}'
            """)

            columns = []
            for col in cursor.fetchall():
                columns.append({
                    "name": col[0],
                    "type": col[1],
                    "nullable": col[2] == 'YES'
                })

            # Get sample data
            cursor.execute(f"SELECT * FROM {catalog}.{schema}.{table} LIMIT 10")
            sample_data = []
            if cursor.description:
                col_names = [desc[0] for desc in cursor.description]
                for row in cursor:
                    # Convert complex types like Decimal, date, datetime to string for JSON compatibility
                    processed_row = [
                        str(item) if isinstance(item, (Decimal, date, datetime)) else item
                        for item in row
                    ]
                    sample_data.append(dict(zip(col_names, processed_row)))

            return {
                "name": table,
                "schema": schema,
                "catalog": catalog,
                "columns": columns,
                "sample_data": sample_data,
                "extraction_timestamp": datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"Failed to get table info for {catalog}.{schema}.{table}: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()

    def execute_query(self, query: str, output_file_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute a query. If output_file_path is provided, write results to CSV,
        otherwise return results as list of dictionaries (use with caution for large results).

        Args:
            query (str): The SQL query to execute.
            output_file_path (Optional[str]): Path to save results as CSV.

        Returns:
            Dict[str, Any]: Execution metadata including row_count and optionally output_file_path or results list.
        """
        if not self._connection:
            self.connect() # Ensure connection

        cursor = None
        row_count = 0
        column_names = []
        results_list = [] # Only used if output_file_path is None

        try:
            cursor = self.cursor()
            cursor.execute(query)

            if not cursor.description:
                logger.info("Query executed but returned no columns/rows.")
                return {"row_count": 0, "output_file_path": output_file_path, "columns": []}

            column_names = [desc[0] for desc in cursor.description]

            if output_file_path:
                # Ensure directory exists
                os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
                with open(output_file_path, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(column_names) # Write header
                    for row in cursor:
                        # Convert complex types for CSV writing if necessary (though csv module handles most)
                        processed_row = [
                             str(item) if isinstance(item, (Decimal, date, datetime)) else item
                             for item in row
                        ]
                        writer.writerow(processed_row)
                        row_count += 1
                logger.info(f"Query results written to {output_file_path}")
                return {"row_count": row_count, "output_file_path": output_file_path, "columns": column_names}
            else:
                # Return results directly (use cautiously)
                logger.warning("Returning query results directly in memory. This may fail for large datasets.")
                for row in cursor:
                     processed_row = [
                         str(item) if isinstance(item, (Decimal, date, datetime)) else item
                         for item in row
                     ]
                     results_list.append(dict(zip(column_names, processed_row)))
                     row_count += 1
                return {"row_count": row_count, "results": results_list, "columns": column_names}

        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()

    def close(self):
        """Close the current connection"""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Trino connection closed.")
        if self._sqlalchemy_engine:
            self._sqlalchemy_engine.dispose()
            self._sqlalchemy_engine = None
        self.current_catalog = None
        self.current_schema = None

    def delete_connection(self, connection_id: str) -> bool:
        """
        Delete a saved connection

        Args:
            connection_id (str): ID of the connection to delete

        Returns:
            bool: True if connection was deleted, False if connection not found
        """
        try:
            # Close connection if it's active
            if self.connection:
                self.close()

            # Delete from storage
            if self.connection_storage:
                return self.connection_storage.delete_connection(connection_id)
            else:
                logger.warning("No connection storage configured, cannot delete.")
                return False

        except Exception as e:
            logger.error(f"Failed to delete connection: {str(e)}")
            raise

    def list_connections(self) -> List[str]:
        """
        List all available connection IDs

        Returns:
            List[str]: List of connection IDs
        """
        if self.connection_storage:
            return self.connection_storage.list_connections()
        else:
            logger.warning("No connection storage configured, cannot list connections.")
            return []

    def get_table_metadata(self, catalog: str, schema: str, table: str) -> Dict:
        """Get basic table metadata - delegates to metadata extractor for detailed info"""
        self.connect_to_catalog(catalog, schema)
        cursor = None
        try:
            cursor = self.cursor()
            # More robust check using information_schema
            cursor.execute(f"""
                SELECT count(*)
                FROM {catalog}.information_schema.tables
                WHERE table_catalog = '{catalog}'
                AND table_schema = '{schema}'
                AND table_name = '{table}'
            """)
            exists = cursor.fetchone()[0] > 0
            return {
                'exists': exists,
                'accessible': exists # Assumes existence implies accessibility for this basic check
            }
        except Exception as e:
            logger.error(f"Error checking table {catalog}.{schema}.{table}: {str(e)}")
            # Attempt to determine if the error is due to non-existence vs other issues
            # This is complex and database-specific; simplified check here
            return {
                'exists': False,
                'accessible': False,
                'error': str(e)
            }
        finally:
            if cursor:
                cursor.close()

    # Removed duplicate get_connection method

    def close_all(self):
        """Close all active connections"""
        # This method might be more relevant at the application level if multiple managers exist
        self.close()

    def __enter__(self):
        """Context manager entry"""
        return self.connect() # Ensure connected on entry

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

# Removed TrinoConnectionConfig class as it seemed incomplete/unused

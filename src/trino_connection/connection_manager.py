from datetime import datetime, date
from decimal import Decimal
import trino
import logging
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
            return self
        except Exception as e:
            logger.error(f"Failed to connect to Trino: {str(e)}")
            raise

    def get_connection(self, connection_id: str) -> Optional['TrinoConnectionManager']:
        """Get a connection by ID"""
        if not self.connection_storage:
            return None
            
        connection_details = self.connection_storage.get_connection(connection_id)
        if not connection_details:
            return None
            
        return TrinoConnectionManager(
            host=connection_details['host'],
            port=connection_details['port'],
            user=connection_details['user'],
            password=connection_details.get('password'),
            http_scheme=connection_details.get('http_scheme', 'http'),
            verify=connection_details.get('verify', True),
            connection_storage=self.connection_storage
        ).connect()

    @property
    def connection(self):
        """Get the current connection"""
        return self._connection

    def cursor(self):
        """Get a cursor from the connection"""
        if not self._connection:
            raise Exception("Not connected to database. Call connect() first.")
        return self._connection.cursor()

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
        if not self._connection:
            raise ValueError("No active connection")

        try:
            cursor = self._connection.cursor()
            
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
                    sample_data.append(dict(zip(col_names, row)))

            return {
                "name": table,
                "schema": schema,
                "catalog": catalog,
                "columns": columns,
                "sample_data": sample_data,
                "extraction_timestamp": datetime.utcnow().isoformat()
            }

        except Exception as e:
            raise Exception(f"Failed to get table info: {str(e)}")
        finally:
            if cursor:
                cursor.close()

    def execute_query(self, query: str) -> List[Dict]:
        """Execute a query and return results as list of dictionaries"""
        if not self._connection:
            raise Exception("Not connected to Trino server")

        cursor = None
        try:
            cursor = self._connection.cursor()
            cursor.execute(query)
            
            if not cursor.description:
                return []
                
            columns = [desc[0] for desc in cursor.description]
            results = []
            for row in cursor:
                results.append(dict(zip(columns, row)))
            return results
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
            return self.connection_storage.delete_connection(connection_id)
            
        except Exception as e:
            logger.error(f"Failed to delete connection: {str(e)}")
            raise

    def list_connections(self) -> List[str]:
        """
        List all available connection IDs
        
        Returns:
            List[str]: List of connection IDs
        """
        return self.connection_storage.list_connections()

    def get_table_metadata(self, catalog: str, schema: str, table: str) -> Dict:
        """Get basic table metadata - delegates to metadata extractor for detailed info"""
        if not self._connection:
            raise ValueError("No active connection")
        
        cursor = None
        try:
            cursor = self._connection.cursor()
            cursor.execute(f"SELECT 1 FROM {catalog}.{schema}.{table} LIMIT 1")
            return {
                'exists': True,
                'accessible': True
            }
        except Exception as e:
            logger.error(f"Error checking table {catalog}.{schema}.{table}: {str(e)}")
            return {
                'exists': False,
                'accessible': False,
                'error': str(e)
            }
        finally:
            if cursor:
                cursor.close()

    def get_connection(self, connection_id: str) -> Optional['TrinoConnectionManager']:
        """
        Get an existing connection by ID
        
        Args:
            connection_id (str): Connection ID
        
        Returns:
            Optional[TrinoConnectionManager]: Connection manager instance or None if not found
        """
        try:
            # Get connection details from storage
            connection_details = self.connection_storage.get_connection(connection_id)
            if not connection_details:
                logger.warning(f"No connection found for ID: {connection_id}")
                return None
            
            # Create new connection manager instance with stored details
            connection = TrinoConnectionManager(
                host=connection_details['host'],
                port=connection_details['port'],
                user=connection_details['user'],
                password=connection_details.get('password'),
                http_scheme=connection_details.get('http_scheme', 'http'),
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
            logger.error(f"Failed to get connection {connection_id}: {str(e)}")
            raise

    def close_all(self):
        """Close all active connections"""
        try:
            if hasattr(self, 'connection') and self.connection:
                self.connection.close()
        except Exception as e:
            logger.error(f"Error closing connections: {str(e)}")

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

class TrinoConnectionConfig:
    @staticmethod
    def save_connection(connection_details: Dict):
        """Save Trino connection details securely"""
        # TODO: Implement secure storage mechanism
        pass
    
    @staticmethod
    def load_connection(connection_name: str) -> Optional[Dict]:
        """Load saved Trino connection details"""
        # TODO: Implement secure retrieval mechanism
        return None

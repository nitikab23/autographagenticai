import json
import os
from typing import Dict, Optional
import logging

class SecureConnectionStorage:
    """Manages storage and retrieval of Trino connection configurations"""
    
    def __init__(self, storage_path: str = None):
        if storage_path is None:
            storage_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                'storage',
                'metadata',
                'connections'
            )
        
        self.storage_path = storage_path
        self.connections_file = os.path.join(storage_path, 'connections.json')
        
        os.makedirs(storage_path, exist_ok=True)
        
        if not os.path.exists(self.connections_file):
            self._save_connections({})

    def _load_connections(self) -> Dict:
        """Load connections from storage"""
        try:
            if os.path.exists(self.connections_file):
                with open(self.connections_file, 'r') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            logging.error(f"Error loading connections: {str(e)}")
            return {}

    def _save_connections(self, connections: Dict) -> None:
        """Save connections to storage"""
        try:
            with open(self.connections_file, 'w') as f:
                json.dump(connections, f, indent=2)
        except Exception as e:
            logging.error(f"Error saving connections: {str(e)}")
            raise

    def store_connection(self, connection_id: str, connection_details: Dict) -> None:
        """Store a new connection configuration"""
        connections = self._load_connections()
        connections[connection_id] = connection_details
        self._save_connections(connections)

    def get_connection(self, connection_id: str) -> Optional[Dict]:
        """Retrieve a connection configuration by ID"""
        connections = self._load_connections()
        return connections.get(connection_id)

    def list_connections(self) -> Dict:
        """List all stored connections"""
        return self._load_connections()

    def delete_connection(self, connection_id: str) -> bool:
        """Delete a connection configuration"""
        connections = self._load_connections()
        if connection_id in connections:
            del connections[connection_id]
            self._save_connections(connections)
            return True
        return False

    def update_connection(self, connection_id: str, connection_details: Dict) -> bool:
        """Update an existing connection configuration"""
        connections = self._load_connections()
        if connection_id in connections:
            connections[connection_id].update(connection_details)
            self._save_connections(connections)
            return True
        return False

# For backward compatibility
ConnectionStorage = SecureConnectionStorage

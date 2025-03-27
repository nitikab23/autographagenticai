from dataclasses import dataclass
from typing import List, Dict, Any
from datetime import datetime

@dataclass
class TableMetadata:
    """Represents complete table metadata"""
    name: str
    schema: str
    catalog: str
    columns: List[Dict]
    primary_keys: List[str]
    foreign_keys: List[Dict]
    relationships: List[Dict]
    sample_data: List[Dict]
    description: str  # This field should be populated by LLM
    table_type: str
    general_info: Dict[str, Any]

    def update_with_llm_enrichment(self, table_description: str, enriched_columns: List[Dict]):
        """Update metadata with LLM-generated descriptions"""
        self.description = table_description
        self.columns = enriched_columns
        
    def to_dict(self) -> Dict:
        """Convert metadata to dictionary"""
        return {
            'name': self.name,
            'schema': self.schema,
            'catalog': self.catalog,
            'columns': self.columns,
            'primary_keys': self.primary_keys,
            'foreign_keys': self.foreign_keys,
            'relationships': self.relationships,
            'sample_data': self.sample_data,
            'description': self.description,
            'table_type': self.table_type,
            'general_info': self.general_info
        }

    def values(self):
        """Return dictionary values"""
        return self.to_dict().values()

    def items(self):
        """Return dictionary items"""
        return self.to_dict().items()

    def update(self, other: Dict):
        """Update metadata with new values"""
        for key, value in other.items():
            self[key] = value

    @staticmethod
    def create_default(catalog: str, schema: str, table: str) -> 'TableMetadata':
        return TableMetadata(
            name=table,
            schema=schema,
            catalog=catalog,
            columns=[],
            primary_keys=[],
            foreign_keys=[],
            relationships=[],
            sample_data=[],
            description=f"Table {table} in {catalog}.{schema}",
            table_type="BASE TABLE",
            general_info={
                "row_count": None,
                "size_bytes": None,
                "last_analyzed": datetime.now().isoformat()
            }
        )

    def to_project_summary(self) -> Dict:
        """Convert to project summary format"""
        return {
            "name": self.name,
            "schema": self.schema,
            "catalog": self.catalog,
            "columns": self.columns,
            "primary_keys": self.primary_keys,
            "relationships": self.relationships,
            "sample_data": self.sample_data,
            "description": self.description,
            "row_count": self.general_info.get("row_count"),
            "last_analyzed": self.general_info.get("last_analyzed")
        }

from typing import Dict, List
from openai import OpenAI
import logging
import json

logger = logging.getLogger(__name__)

class MetadataEnricher:
    def __init__(self, client: OpenAI):
        self.client = client
        self.logger = logging.getLogger(__name__)

    def enrich_table_metadata(self, table_name: str, columns: List[Dict], sample_data: List[Dict]) -> str:
        """Generate table description using LLM"""
        try:
            self.logger.debug(f"Starting table metadata enrichment for {table_name}")
            
            # Create a more detailed prompt
            column_info = "\n".join([
                f"- {col['name']} ({col['type']}{' - nullable' if col.get('nullable') else ''})"
                for col in columns
            ])
            
            # Limit sample data to prevent token overflow
            safe_sample_data = []
            for row in sample_data[:2]:
                safe_row = {}
                for k, v in row.items():
                    # Convert to string and truncate if too long
                    safe_row[k] = str(v)[:100] if v is not None else None
                safe_sample_data.append(safe_row)
            
            sample_data_str = json.dumps(safe_sample_data, indent=2)
            
            prompt = f"""Analyze this database table and provide a clear, concise description:

Table: {table_name}

Columns:
{column_info}

Sample Data:
{sample_data_str}

Provide a concise (2-3 sentences) technical description of this table's purpose and contents."""

            self.logger.debug(f"Sending prompt to OpenAI: {prompt}")

            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a database analyst providing technical descriptions of database tables."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,  # Lower temperature for more consistent output
                max_tokens=200
            )

            description = response.choices[0].message.content.strip()
            self.logger.info(f"Generated table description: {description}")
            return description

        except Exception as e:
            self.logger.error(f"Failed to generate table description: {str(e)}", exc_info=True)
            return ""  # Return empty string instead of error message

    def enrich_column_metadata(self, table_name: str, columns: List[Dict], sample_data: List[Dict]) -> List[Dict]:
        """Generate descriptions for each column using LLM"""
        try:
            enriched_columns = []
            for column in columns:
                try:
                    # Get sample values for this column
                    sample_values = [
                        str(row.get(column['name']))[:50]  # Truncate long values
                        for row in sample_data[:3]
                        if row.get(column['name']) is not None
                    ]
                    
                    prompt = f"""Analyze this database column and provide a clear, concise description:

Table: {table_name}
Column: {column['name']}
Data Type: {column['type']}
Nullable: {column.get('nullable', True)}
Sample Values: {', '.join(sample_values)}

Provide a concise (1 sentence) technical description of this column's purpose and contents."""

                    self.logger.debug(f"Sending prompt for column {column['name']}")
                    
                    response = self.client.chat.completions.create(
                        model="gpt-3.5-turbo",
                        messages=[
                            {"role": "system", "content": "You are a database analyst providing technical descriptions of database columns."},
                            {"role": "user", "content": prompt}
                        ],
                        temperature=0.3,
                        max_tokens=100
                    )

                    column_description = response.choices[0].message.content.strip()
                    self.logger.info(f"Generated description for column {column['name']}: {column_description}")
                    
                    enriched_column = column.copy()
                    enriched_column['description'] = column_description
                    enriched_columns.append(enriched_column)
                    
                except Exception as e:
                    self.logger.error(f"Failed to enrich column {column['name']}: {str(e)}")
                    enriched_column = column.copy()
                    enriched_column['description'] = ""
                    enriched_columns.append(enriched_column)

            return enriched_columns

        except Exception as e:
            self.logger.error(f"Failed to generate column descriptions: {str(e)}")
            return columns  # Return original columns if enrichment fails

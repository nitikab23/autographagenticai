from typing import Dict
from src.agents.core import Agent, ReasoningStep
from src.agents.context import ContextProtocol

class ExtractorAgent(Agent):
    def __init__(self, context: ContextProtocol):
        super().__init__(context)

    async def execute(self) -> ReasoningStep:
        try:
            metadata = self.context.get("metadata", {})
            if not metadata or not metadata.get("tables"):
                return ReasoningStep(
                    agent="ExtractorAgent",
                    operation="error",
                    details={"error": "No metadata or tables available in context"}
                )

            sql_query = self._generate_raw_sql(metadata)
            result = ReasoningStep(
                agent="ExtractorAgent",
                operation="sql_generation",
                details={"sql_query": sql_query}
            )
            self._log_step(result)
            return result
        except Exception as e:
            self.logger.error(f"SQL generation failed: {str(e)}")
            return ReasoningStep(
                agent="ExtractorAgent",
                operation="error",
                details={"error": str(e)}
            )

    def _generate_raw_sql(self, metadata: Dict) -> str:
        """
        Generate a raw SQL query from metadata, pulling columns from metadata.columns and drill_down,
        excluding duplicates, and including all joins.
        
        Args:
            metadata (Dict): Metadata containing tables, columns, joins, and drill_down.
        
        Returns:
            str: Raw SQL query string.
        """
        # Collect columns from metadata.columns, avoiding duplicates
        seen_columns = set()
        select_cols = []
        
        # Start with columns from metadata.columns
        for table, columns in metadata.get("columns", {}).items():
            for col in columns:
                full_name = f"{table}.{col['name']}"
                if full_name not in seen_columns:
                    seen_columns.add(full_name)
                    select_cols.append(full_name)
                    self.logger.debug(f"Added column from metadata: {full_name}")
        
        # Add drill-down columns/expressions, skipping duplicates
        for level in metadata.get("drill_down", {}).get("levels", []):
            for col in level.get("columns", []):
                # Handle expressions (e.g., CONCAT) or simple columns
                if " AS " in col:
                    # Extract the alias to check for uniqueness
                    alias = col.split(" AS ")[-1].strip()
                    if alias not in seen_columns:
                        seen_columns.add(alias)
                        select_cols.append(col)
                        self.logger.debug(f"Added drill-down expression: {col}")
                else:
                    # Simple column (e.g., postgresql.public.category.name)
                    if col not in seen_columns:
                        seen_columns.add(col)
                        select_cols.append(col)
                        self.logger.debug(f"Added drill-down column: {col}")
        
        # Generate JOIN clauses
        joins = "\n".join(
            f"{join['type']} JOIN {join['right_table']} ON {join['left_table']}.{join['left_column']} = {join['right_table']}.{join['right_column']}"
            for join in metadata.get("joins", [])
        )
        
        # Ensure there's at least one table to start the FROM clause
        base_table = metadata.get("tables", [])[0] if metadata.get("tables") else "hive.rentals_db.payment"
        
        # Construct the query
        query = f"SELECT {', '.join(select_cols)}\nFROM {base_table}\n{joins}"
        self.logger.debug(f"Generated raw SQL: {query}")
        return query
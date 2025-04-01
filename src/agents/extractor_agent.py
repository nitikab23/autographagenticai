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

            sql_query = self._generate_sql(metadata)
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

    def _generate_sql(self, metadata: Dict) -> str:
        tables = metadata.get("tables", [])
        joins = metadata.get("joins", [])
        filters = metadata.get("filters", [])
        drill_down = metadata.get("drill_down", {}).get("levels", [])

        # Select raw columns: drill_down + ID columns
        drill_down_cols = [col for level in drill_down for col in level["columns"]]
        id_cols = [
            "postgresql.public.category.category_id",
            "postgresql.public.actor.actor_id",
            "hive.rentals_db.payment.rental_id"  # Include rental_id for completeness
        ]
        select_cols = list(set(drill_down_cols + id_cols))
        if not select_cols:
            raise ValueError("No columns specified for SELECT")
        select_clause = "SELECT " + ",\n  ".join(select_cols)

        if not tables:
            raise ValueError("No tables specified")
        from_clause = f"FROM {tables[0]}"
        join_clause = "\n".join([
            f"LEFT JOIN {join['right_table']} ON {join['left_table']}.{join['left_column']} = {join['right_table']}.{join['right_column']}"
            for join in joins
        ])

        where_conditions = [f"{f['column']} {f['condition']}" for f in filters]
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

        sql_parts = [select_clause, from_clause]
        if join_clause:
            sql_parts.append(join_clause)
        if where_clause:
            sql_parts.append(where_clause)

        return "\n".join(sql_parts).strip()
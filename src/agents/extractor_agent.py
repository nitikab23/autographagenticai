from typing import Dict
from src.agents.core import Agent, ReasoningStep
from src.agents.context import ContextProtocol

class ExtractorAgent(Agent):
    def __init__(self, context: ContextProtocol):
        super().__init__(context)

    async def execute(self) -> ReasoningStep:
        try:
            # The entire plan from MetadataAgent should be under the 'metadata' key
            metadata_plan = self.context.get("metadata", {})
            # Check for the presence of essential keys from the new plan structure
            if not metadata_plan or not metadata_plan.get("initial_tables"):
                self.logger.error(f"ExtractorAgent Error: Missing 'initial_tables' or metadata plan in context. Context keys: {list(self.context.snapshot().keys())}")
                return ReasoningStep(
                    agent="ExtractorAgent",
                    operation="error",
                    # Provide more context in the error message
                    details={"error": "Missing 'initial_tables' key or metadata plan in context."}
                )
            # TODO: Add more robust error handling if other expected keys are missing
            sql_query = self._generate_aggregated_sql(metadata_plan) # Pass the plan object
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

    def _generate_aggregated_sql(self, metadata: Dict) -> str:
        # ... (get group_by_cols, aggregations - still useful for validation/logging if needed) ...

        # *** CORRECTED SELECT CLAUSE LOGIC ***
        planned_select_columns_objs = metadata.get('initial_select_columns', [])
        if not planned_select_columns_objs or not isinstance(planned_select_columns_objs, list):
             raise ValueError("Cannot generate query: 'initial_select_columns' is missing, empty, or not a list in the metadata plan.")

        sql_select_items = []
        for item_obj in planned_select_columns_objs:
            if not isinstance(item_obj, dict) or 'expression' not in item_obj or 'alias' not in item_obj:
                self.logger.warning(f"Skipping invalid item in initial_select_columns: {item_obj}")
                continue

            expression = item_obj['expression']
            alias = item_obj['alias']

            # Ensure expression and alias are strings
            if not isinstance(expression, str) or not isinstance(alias, str):
                 self.logger.warning(f"Skipping item with non-string expression/alias in initial_select_columns: {item_obj}")
                 continue

            # Construct the SELECT item string: "expression AS alias"
            # Note: Even if expression == alias (like referencing an aggregate),
            #       using "alias AS alias" is valid SQL, though slightly redundant.
            #       Let's keep it simple: always use "expression AS alias".
            #       Alternatively, you could check if expression == alias and just add expression,
            #       but "AS" is generally safer if expression could be complex.
            sql_select_items.append(f"{expression} AS {alias}")

        if not sql_select_items:
             raise ValueError("Cannot generate query: SQL SELECT list is empty after processing initial_select_columns.")

        select_clause = f"SELECT {', '.join(sql_select_items)}"
        self.logger.debug(f"SELECT clause: {select_clause}")
        # *** END CORRECTED SELECT CLAUSE LOGIC ***


        # Determine base table (use left table of first join or first initial table)
        joins_list = metadata.get("initial_joins", [])
        if joins_list and isinstance(joins_list, list) and len(joins_list) > 0 and isinstance(joins_list[0], dict) and 'left_table' in joins_list[0]:
             base_table = joins_list[0]['left_table']
        elif metadata.get("initial_tables") and isinstance(metadata.get("initial_tables"), list) and len(metadata.get("initial_tables")) > 0:
             base_table = metadata.get("initial_tables")[0]
        else:
             # Fallback: Try to infer from group by or aggregations if desperate, or raise error.
             # For simplicity, let's raise an error. The MetadataAgent *should* provide tables/joins.
             self.logger.error("Cannot determine base table: No joins or initial tables found in metadata plan.")
             raise ValueError("Cannot determine base table: No joins or initial tables found in metadata plan.")
        from_clause = f"FROM {base_table}"
        self.logger.debug(f"FROM clause: {from_clause}")


        # --- Generate JOIN clauses ---
        join_clauses_list = []
        processed_tables = {base_table} # Keep track of tables already in FROM/JOIN

        # Determine the relationship structure more carefully if possible.
        # This simple sequential join might be okay for linear paths but complex otherwise.
        # A more robust approach might build a graph, but let's stick to the plan's order.

        # Use range(len()) to potentially access the previous join's right table if needed,
        # but the current metadata format implies a chain starting from the first join's left_table.
        for join in joins_list:
            # Ensure required keys exist
            if not all(k in join for k in ['type', 'right_table', 'left_table', 'left_column', 'right_column']):
                 self.logger.warning(f"Skipping invalid join structure: {join}")
                 continue

            # Construct fully qualified column names for the ON clause
            # Assumes short names are provided in the plan as per the required format
            on_clause = f"{join['left_table']}.{join['left_column']} = {join['right_table']}.{join['right_column']}"

            # Add the clause
            join_clauses_list.append(f"{join['type'].upper()} JOIN {join['right_table']} ON {on_clause}")
            processed_tables.add(join['right_table'])

        join_clauses = "\n".join(join_clauses_list)
        self.logger.debug(f"JOIN clauses:\n{join_clauses}")


        # --- Generate WHERE clause ---
        filters = metadata.get("initial_filters", [])
        where_clause = ""
        if filters:
            # Ensure filter is a dict with column and condition
            valid_filters = [f" {f['condition']}" for f in filters if isinstance(f, dict) and 'condition' in f]
            if valid_filters:
                 conditions = " AND ".join(valid_filters)
                 where_clause = f"WHERE {conditions}"
        self.logger.debug(f"WHERE clause: {where_clause}")


        # --- Generate GROUP BY clause ---
        group_by_cols = metadata.get("group_by_columns", [])
        group_by_clause = ""
        if group_by_cols:
            # Use explicit column names/expressions from the plan for GROUP BY
            group_by_clause = f"GROUP BY {', '.join(group_by_cols)}"
        self.logger.debug(f"GROUP BY clause: {group_by_clause}")


        # --- Generate HAVING clause ---
        # (Your existing logic for HAVING is fine)
        having_conditions = metadata.get("having_conditions", [])
        having_clause = ""
        if having_conditions:
            conditions = " AND ".join(cond.get("condition", "") for cond in having_conditions if isinstance(cond, dict) and cond.get("condition"))
            if conditions:
                having_clause = f"HAVING {conditions}"
        self.logger.debug(f"HAVING clause: {having_clause}")


        # --- Generate ORDER BY clause ---
        order_by_spec_input = metadata.get("order_by_spec") # Get the input (could be dict or list)
        order_by_clause = ""
        sort_terms = [] # List to hold individual "column direction" strings

        # Normalize input: Ensure we always have a list to iterate over
        order_by_spec_list = []
        if isinstance(order_by_spec_input, dict):
            order_by_spec_list = [order_by_spec_input] # Wrap single dict in a list
        elif isinstance(order_by_spec_input, list):
            order_by_spec_list = order_by_spec_input # Use the list directly

        # Process the list (now guaranteed to be a list, possibly empty)
        if order_by_spec_list:
            for spec in order_by_spec_list:
                # Validate each item in the list
                if isinstance(spec, dict) and spec.get("column"):
                    column = spec["column"]
                    direction = spec.get("direction", "ASC").upper()
                    if direction not in ["ASC", "DESC"]:
                        direction = "ASC"
                    sort_terms.append(f"{column} {direction}")
                else:
                    self.logger.warning(f"Skipping invalid item in order_by_spec list: {spec}")

        # If we found valid sort terms, construct the clause
        if sort_terms:
            order_by_clause = f"ORDER BY {', '.join(sort_terms)}"

        self.logger.debug(f"ORDER BY clause: {order_by_clause}")


        # --- Generate LIMIT clause ---
        # (Your existing logic for LIMIT is fine)
        limit_count = metadata.get("limit_count")
        limit_clause = ""
        if limit_count is not None:
            try:
                 limit_val = int(limit_count)
                 if limit_val > 0: limit_clause = f"LIMIT {limit_val}"
                 else: self.logger.warning(f"Ignoring non-positive LIMIT value: {limit_count}")
            except (ValueError, TypeError):
                 self.logger.warning(f"Ignoring invalid LIMIT value type: {limit_count}")
        self.logger.debug(f"LIMIT clause: {limit_clause}")


        # --- Construct the final query ---
        query_parts = [
            select_clause,
            from_clause,
            join_clauses,
            where_clause,
            group_by_clause,
            having_clause,
            order_by_clause,
            limit_clause,
        ]
        query = "\n".join(part for part in query_parts if part) # Join non-empty parts
        self.logger.info(f"Generated aggregated SQL:\n{query}") # Use INFO or DEBUG
        return query

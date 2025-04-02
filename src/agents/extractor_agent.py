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
        """
        Generate an aggregated SQL query based on the structured plan from MetadataAgent.

        Args:
            metadata (Dict): The structured plan containing initial_select_columns,
                             initial_tables, initial_joins, initial_filters, group_by_columns.

        Returns:
            str: Aggregated SQL query string.
        """
        group_by_cols = metadata.get("group_by_columns", [])
        aggregations = metadata.get("initial_aggregations", [])
        # Also get the intended final select list to find calculated fields
        planned_select_columns = metadata.get('initial_select_columns', [])

        # Construct the actual SQL SELECT list
        sql_select_list = []
        # 1. Add grouping columns
        sql_select_list.extend(group_by_cols)

        # 2. Add aggregation expressions with aliases
        aggregation_aliases = set()
        for agg in aggregations:
            if agg.get("expression") and agg.get("alias"):
                sql_select_list.append(f"{agg['expression']} AS {agg['alias']}")
                aggregation_aliases.add(agg['alias'])
            else:
                 self.logger.warning(f"Skipping invalid aggregation item: {agg}")

        # 3. Add any calculated fields from the planned select list that are not
        #    grouping columns or simple aggregation aliases.
        #    Also validate consistency with GROUP BY rules.
        group_by_set = set(group_by_cols)
        processed_aliases = set(agg['alias'] for agg in aggregations if agg.get('alias')) # Track aliases from Step 2

        for item in planned_select_columns:
            is_grouping_col = item in group_by_set
            is_processed_agg_alias = item in processed_aliases

            # If it's not a grouping column and not an alias from an aggregation we already added
            if not is_grouping_col and not is_processed_agg_alias:
                # Check if it's a calculated field (contains ' AS ') or just a raw column
                if ' AS ' in item:
                    # It's a calculated field, add it
                    sql_select_list.append(item)
                    self.logger.debug(f"Adding calculated select item: {item}")
                else:
                    # It's a raw column/alias not in GROUP BY and not from an aggregation. This violates SQL rules.
                    # Raise an error indicating an inconsistent plan from MetadataAgent.
                    error_msg = (
                        f"Inconsistent plan: SELECT item '{item}' is not in GROUP BY columns "
                        f"({group_by_cols}) and is not a defined aggregation alias ({processed_aliases}). "
                        f"All non-aggregated SELECT items must be in the GROUP BY clause."
                    )
                    self.logger.error(error_msg)
                    raise ValueError(error_msg)

        if not sql_select_list:
             raise ValueError("Cannot generate query: SQL SELECT list is empty after processing plan.")
        # Remove duplicates just in case (e.g., if a group_by col was also in planned_select_columns)
        # Using dict.fromkeys preserves order in Python 3.7+
        unique_sql_select_list = list(dict.fromkeys(sql_select_list))
        select_clause = f"SELECT {', '.join(unique_sql_select_list)}"
        self.logger.debug(f"SELECT clause: {select_clause}")


        # Determine base table (use left table of first join or first initial table)
        joins_list = metadata.get("initial_joins", [])
        if joins_list:
            base_table = joins_list[0]['left_table']
        elif metadata.get("initial_tables"):
            base_table = metadata.get("initial_tables")[0]
        else:
            # Fallback or raise error - should not happen if MetadataAgent works
            raise ValueError("Cannot determine base table: No joins or initial tables found in metadata plan.")
        from_clause = f"FROM {base_table}"
        self.logger.debug(f"FROM clause: {from_clause}")

        # Generate JOIN clauses
        join_clauses = "\n".join(
            # Construct fully qualified column names for the ON clause
            f"{join['type']} JOIN {join['right_table']} ON {join['left_table']}.{join['left_column']} = {join['right_table']}.{join['right_column']}"
            for join in joins_list
        )
        self.logger.debug(f"JOIN clauses:\n{join_clauses}")

        # Generate WHERE clause
        filters = metadata.get("initial_filters", [])
        where_clause = ""
        if filters:
            conditions = " AND ".join(f"{f['column']} {f['condition']}" for f in filters)
            where_clause = f"WHERE {conditions}"
        self.logger.debug(f"WHERE clause: {where_clause}")

        # Generate GROUP BY clause
        # group_by_cols already fetched above

        group_by_clause = ""
        if group_by_cols:
            # Use explicit column names/expressions from the plan for GROUP BY
            group_by_clause = f"GROUP BY {', '.join(group_by_cols)}"
        self.logger.debug(f"GROUP BY clause: {group_by_clause}")


        # Generate HAVING clause
        having_conditions = metadata.get("having_conditions", [])
        having_clause = ""
        if having_conditions:
            # Assuming each item in having_conditions is an object like {'condition': 'alias > value'}
            conditions = " AND ".join(cond.get("condition", "") for cond in having_conditions if cond.get("condition"))
            if conditions:
                having_clause = f"HAVING {conditions}"
                self.logger.debug(f"HAVING clause: {having_clause}")
            else:
                self.logger.debug("HAVING conditions provided but were empty or invalid.")
        else:
            self.logger.debug("No HAVING conditions found in metadata plan.")


        # Generate ORDER BY clause
        order_by_spec = metadata.get("order_by_spec")
        order_by_clause = ""
        if order_by_spec and order_by_spec.get("column"):
            column = order_by_spec["column"]
            direction = order_by_spec.get("direction", "ASC").upper() # Default to ASC
            if direction not in ["ASC", "DESC"]:
                direction = "ASC" # Fallback for invalid direction
            order_by_clause = f"ORDER BY {column} {direction}"
            self.logger.debug(f"ORDER BY clause: {order_by_clause}")
        else:
             self.logger.debug("No ORDER BY specification found in metadata plan.")

        # Generate LIMIT clause
        limit_count = metadata.get("limit_count")
        limit_clause = ""
        if limit_count is not None:
            try:
                limit_val = int(limit_count)
                if limit_val > 0:
                    limit_clause = f"LIMIT {limit_val}"
                    self.logger.debug(f"LIMIT clause: {limit_clause}")
                else:
                    self.logger.warning(f"Invalid limit_count value ({limit_count}), must be positive. Ignoring LIMIT.")
            except (ValueError, TypeError):
                 self.logger.warning(f"Invalid limit_count type ({type(limit_count)}), expected integer. Ignoring LIMIT.")
        else:
            self.logger.debug("No LIMIT specification found in metadata plan.")


        # Construct the final query
        query_parts = [
            select_clause,
            from_clause,
            join_clauses,
            where_clause,
            group_by_clause,
            having_clause,     # HAVING comes after GROUP BY
            order_by_clause,   # ORDER BY comes after HAVING
            limit_clause,      # LIMIT comes last
        ]
        query = "\n".join(part for part in query_parts if part) # Join non-empty parts
        self.logger.debug(f"Generated aggregated SQL:\n{query}")
        return query

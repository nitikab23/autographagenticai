import csv
from typing import Dict, List, Any
import logging
import json # Import json for snapshot logging
import os # Import os for path joining
import time # Import time for timestamped filenames
from pathlib import Path # Import Path for directory creation
from src.agents.core import Agent, ReasoningStep
from src.agents.context import ContextProtocol
from .visualization_agent import VisualizationAgent # Added import
# Import the connection manager
from src.trino_connection.connection_manager import TrinoConnectionManager
# Assuming ConnectionConfig might be used elsewhere to load env vars into context
# from src.trino_connection.connection_config import ConnectionConfig

class CoordinatorAgent(Agent):
    # Accept TrinoConnectionManager during initialization
    def __init__(self, context: ContextProtocol, connection_manager: TrinoConnectionManager):
        super().__init__(context)
        self.workflow_state = self.context.get("workflow_state", "ANALYZE")
        self.conn_manager = connection_manager # Store the injected connection manager
        # Remove TRANSFORM state for now
        self.components = {
            "ANALYZE": self._analyze_phase,
            "EXTRACT": self._extract_phase,
            "VISUALIZE": self._visualize_phase, # Added VISUALIZE state
            "COMPLETE": self._complete_phase
        }
        # TODO: Ensure Trino connection details are loaded into the context
        # Example: self.context should contain keys like "TRINO_HOST", "TRINO_PORT", etc.

    async def execute(self) -> ReasoningStep:
        step = ReasoningStep(
            agent="Coordinator",
            operation="workflow_init",
            details={"query": self.context.query, "state": self.workflow_state}
        )
        self._log_step(step)

        try:
            # --- Execute current state's component ---
            if self.workflow_state in self.components:
                result = await self.components[self.workflow_state]()
            else:
                 raise ValueError(f"Unknown workflow state: {self.workflow_state}")

            # --- State Transition Logic ---

            # --- ANALYZE Phase Result Handling ---
            if self.workflow_state == "ANALYZE":
                if result.operation == "metadata_analysis":
                    ambiguities = result.details.get("ambiguities", [])
                    unresolved = [amb for amb in ambiguities if amb["question"] not in self.context.get("clarifications", {})]
                    if unresolved:
                        # If unresolved ambiguities, stop and ask for clarification
                        return ReasoningStep(
                            agent="Coordinator",
                            operation="clarification_needed",
                            details={"ambiguities": unresolved}
                        )
                    else:
                        # If resolved or no ambiguities, update context and move to EXTRACT
                        self.workflow_state = "EXTRACT"
                        self.context = self.context.update({
                            "metadata": result.details, # Store the full analysis plan
                            "workflow_state": self.workflow_state
                        })
                        self._log_step(ReasoningStep(
                            agent="Coordinator", operation="state_transition",
                            details={"new_state": self.workflow_state, "reason": "Analysis complete."}
                        ))
                        # Immediately proceed to the next step in the same execution cycle
                        return await self.execute() # Re-call execute to run EXTRACT phase
                elif result.operation == "error":
                    return result # Propagate error

            # --- EXTRACT Phase Result Handling ---
            elif self.workflow_state == "EXTRACT":
                if result.operation == "sql_generation":
                    sql_query = result.details.get("sql_query")
                    if not sql_query:
                         return ReasoningStep(agent="Coordinator", operation="error", details={"error": "ExtractorAgent did not return SQL query."})

                    # --- Execute Query and Save to File ---
                    execution_metadata: Dict[str, Any] = {}
                    try:
                        if not self.conn_manager:
                            raise ValueError("TrinoConnectionManager was not provided.")

                        # Define output path within project storage
                        project_id = self.context.get("project_id")
                        timestamp = int(time.time())
                        # Create a unique directory for this query's output
                        query_output_dir_name = f"{project_id}_{timestamp}"
                        query_output_dir = Path("storage/results") / query_output_dir_name
                        query_output_dir.mkdir(parents=True, exist_ok=True) # Create the specific query dir
                        self.logger.info(f"Created output directory: {query_output_dir}")

                        # Define CSV path inside the new directory
                        output_file = query_output_dir / "query_result.csv" # Consistent filename inside dir

                        self.conn_manager.connect()
                        self.logger.info(f"Executing SQL query:\n{sql_query}")
                        # Call execute_query with the output path
                        execution_metadata = self.conn_manager.execute_query(
                            query=sql_query,
                            output_file_path=str(output_file) # Pass path as string
                        )
                        retrieved_count = execution_metadata.get("row_count", 0)
                        saved_path = execution_metadata.get("output_file_path")
                        self.logger.info(f"Query executed successfully, {retrieved_count} rows saved to {saved_path}")
                        self._log_step(ReasoningStep(
                            agent="Coordinator", operation="data_retrieval",
                            details={"status": "success", "row_count": retrieved_count, "output_file": saved_path}
                        ))

                        # Update context with file path, row count, and the output directory path
                        self.workflow_state = "VISUALIZE" # Changed from COMPLETE
                        self.context = self.context.update({
                            "workflow_state": self.workflow_state,
                            "sql_query": sql_query,
                            "query_result_path": saved_path, # Store CSV file path
                            "query_output_dir": str(query_output_dir), # Store the directory path for visualization agent
                            "query_result_row_count": retrieved_count # Store row count
                            # Remove "query_results" key if it existed
                        })
                        # Clean up old key if necessary (update might handle this depending on impl)
                        if hasattr(self.context, '_data') and "query_results" in self.context._data:
                             del self.context._data["query_results"]


                        self._log_step(ReasoningStep(
                            agent="Coordinator", operation="state_transition",
                            details={"new_state": self.workflow_state, "reason": "SQL executed, data saved. Proceeding to visualization."} # Updated reason
                        ))
                        # Immediately proceed to VISUALIZE phase
                        return await self.execute() # Re-call execute to run VISUALIZE phase

                    except Exception as e:
                        self.logger.error(f"Trino query execution or file write failed: {str(e)}")
                        return ReasoningStep(agent="Coordinator", operation="error", details={"error": f"Trino query execution/write failed: {str(e)}", "sql_query": sql_query})
                    finally:
                        if self.conn_manager:
                            self.conn_manager.close()

                elif result.operation == "error":
                     return result # Propagate error from Extractor
                else:
                     return ReasoningStep(agent="Coordinator", operation="error", details={"error": f"Unexpected result from EXTRACT phase: {result.operation}"})

            # --- VISUALIZE Phase Result Handling ---
            elif self.workflow_state == "VISUALIZE":
                if result.operation == "visualization_generated":
                    visualization_path = result.details.get("output_image_path")
                    self.logger.info(f"Visualization generated successfully at {visualization_path}")
                    # Transition to COMPLETE
                    self.workflow_state = "COMPLETE"
                    self.context = self.context.update({
                        "workflow_state": self.workflow_state,
                        "output_image_path": visualization_path # Store visualization path
                    })
                    self._log_step(ReasoningStep(
                        agent="Coordinator", operation="state_transition",
                        details={"new_state": self.workflow_state, "reason": "Visualization complete."}
                    ))
                    # Immediately proceed to COMPLETE phase
                    return await self.execute()
                elif result.operation == "error":
                    return result # Propagate error from VisualizationAgent
                else:
                    return ReasoningStep(agent="Coordinator", operation="error", details={"error": f"Unexpected result from VISUALIZE phase: {result.operation}"})


            # --- COMPLETE Phase Result Handling ---
            # If we reach here, it means _complete_phase was just executed
            elif self.workflow_state == "COMPLETE":
                 if result.operation == "complete":
                      return result # Final successful completion step
                 elif result.operation == "error":
                      return result # Propagate error from Complete phase
                 else:
                      return ReasoningStep(agent="Coordinator", operation="error", details={"error": f"Unexpected result from COMPLETE phase: {result.operation}"})

            # --- General Error Handling ---
            if result.operation == "error":
                return result # Propagate errors from any phase if not handled above

            # Fallback for unhandled state/operation combinations
            self.logger.warning(f"Unhandled result operation '{result.operation}' in state '{self.workflow_state}' after phase execution.")
            return result # Return the last result

        except Exception as e:
            self.logger.exception(f"Core workflow execution failed in state {self.workflow_state}: {str(e)}") # Use exception for stack trace
            return ReasoningStep(
                agent="Coordinator",
                operation="error",
                details={"error": f"Core workflow execution failed: {str(e)}"}
            )

    async def _analyze_phase(self) -> ReasoningStep:
        from .metadata_agent import MetadataAgent
        agent = MetadataAgent(self.context)
        result = await agent.execute()
        return result

    async def _extract_phase(self) -> ReasoningStep:
        from .extractor_agent import ExtractorAgent
        agent = ExtractorAgent(self.context)
        result = await agent.execute()
        return result

    async def _visualize_phase(self) -> ReasoningStep:
        self.logger.info("Executing VISUALIZE phase...")
        agent = VisualizationAgent(self.context)
        result = await agent.execute()
        return result

    # Removed _transform_phase method

    async def _complete_phase(self) -> ReasoningStep:
        # This phase now uses the file path, row count, and visualization path
        self.logger.info("Executing COMPLETE phase...")
        result_path = self.context.get("query_result_path")
        row_count = self.context.get("query_result_row_count", 0)
        self.logger.debug(f"Data in context at COMPLETE phase - path: {result_path}, row_count: {row_count}")

        # Optionally read a sample from the file for the final message
        final_data_sample = []
        if result_path and os.path.exists(result_path):
             try:
                 with open(result_path, 'r', newline='', encoding='utf-8') as csvfile:
                     reader = csv.reader(csvfile)
                     header = next(reader, None) # Skip header
                     for i, row in enumerate(reader):
                         if i < 5: # Read up to 5 sample rows
                             final_data_sample.append(row)
                         else:
                             break
             except Exception as e:
                 self.logger.warning(f"Could not read sample data from {result_path}: {e}")

        visualization_path = self.context.get("output_image_path")
        message = f"Workflow completed successfully. Results saved to {result_path}"
        if visualization_path:
            message += f" Visualization saved to {visualization_path}"

        return ReasoningStep(
            agent="Coordinator",
            operation="complete",
            details={
                "message": message, # Updated message
                "output_file": result_path,
                "row_count": row_count,
                "visualization_path": visualization_path, # Added visualization path
                "final_data_sample": final_data_sample # Show sample read from file
                }
        )

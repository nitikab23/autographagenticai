from typing import Dict
import logging
from src.agents.core import Agent, ReasoningStep
from src.agents.context import ContextProtocol

class CoordinatorAgent(Agent):
    def __init__(self, context: ContextProtocol):
        super().__init__(context)
        self.workflow_state = self.context.get("workflow_state", "ANALYZE")
        self.components = {
            "ANALYZE": self._analyze_phase,
            "EXTRACT": self._extract_phase,
            "COMPLETE": self._complete_phase
        }

    async def execute(self) -> ReasoningStep:
        step = ReasoningStep(
            agent="Coordinator",
            operation="workflow_init",
            details={"query": self.context.query, "state": self.workflow_state}
        )
        self._log_step(step)

        try:
            if self.workflow_state in self.components:
                result = await self.components[self.workflow_state]()
                if self.workflow_state == "ANALYZE" and result.operation == "metadata_analysis":
                    ambiguities = result.details.get("ambiguities", [])
                    # Check if all ambiguities are resolved
                    unresolved = [amb for amb in ambiguities if amb["question"] not in self.context.get("clarifications", {})]
                    if unresolved:
                        return ReasoningStep(
                            agent="Coordinator",
                            operation="clarification_needed",
                            details={"ambiguities": unresolved}
                        )
                    else:
                        self.workflow_state = "EXTRACT"
                        self.context = self.context.update({
                            "metadata": result.details,
                            "workflow_state": self.workflow_state
                        })
                elif self.workflow_state == "EXTRACT" and result.operation == "sql_generation":
                    self.workflow_state = "COMPLETE"
                    self.context = self.context.update({
                        "workflow_state": self.workflow_state,
                        "sql_query": result.details["sql_query"],
                        "suggested_filters": result.details.get("suggested_filters", [])
                    })
                elif result.operation == "error":
                    return result
                elif result.operation == "complete":
                    return result
                return result
            raise ValueError(f"Unknown state: {self.workflow_state}")
        except Exception as e:
            self.logger.error(f"Workflow failed: {str(e)}")
            return ReasoningStep(
                agent="Coordinator",
                operation="error",
                details={"error": str(e)}
            )

    async def _analyze_phase(self) -> ReasoningStep:
        from .metadata_agent import MetadataAgent
        agent = MetadataAgent(self.context)
        result = await agent.execute()
        self._log_step(result)
        return result

    async def _extract_phase(self) -> ReasoningStep:
        from .extractor_agent import ExtractorAgent
        agent = ExtractorAgent(self.context)
        result = await agent.execute()
        if result.operation == "sql_generation":
            self._log_step(result)
        return result

    async def _complete_phase(self) -> ReasoningStep:
        return ReasoningStep(
            agent="Coordinator",
            operation="complete",
            details={"message": "Workflow completed successfully", "sql_query": self.context.get("sql_query")}
        )
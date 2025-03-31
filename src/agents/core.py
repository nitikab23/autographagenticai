from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import pandas as pd
import logging
from src.agents.context import ContextProtocol
import json
import os

@dataclass
class ReasoningStep:
    agent: str
    operation: str 
    details: Dict[str, Any]
    output: Optional[pd.DataFrame] = None

class Agent(ABC):
    def __init__(self, context: 'ContextProtocol'):
        self.context = context
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    async def execute(self) -> ReasoningStep:
        pass

    def _log_step(self, step: ReasoningStep) -> None:
        self.context.log_step(self.__class__.__name__, step.operation, step.details)
        self.logger.info(f"Agent {self.__class__.__name__} performed {step.operation} with details: {step.details}")

class CoordinatorAgent(Agent):
    def __init__(self, context: 'ContextProtocol'):
        super().__init__(context)
        self.workflow_state = "ANALYZE"
        self.max_retries = 3
        self.components = {
            "ANALYZE": self._analyze_phase,
            "EXTRACT": self._extract_phase,
            "TRANSFORM": self._transform_phase,
            "VISUALIZE": self._visualize_phase
        }
        self.state_validators = {
            "EXTRACT": self._validate_metadata,
            "TRANSFORM": self._validate_raw_data,
            "VISUALIZE": self._validate_transformed_data
        }
        self.logger.info("Using Gemini Metadata Agent")

    async def execute(self) -> ReasoningStep:
        step = ReasoningStep(
            agent="Coordinator",
            operation="workflow_init",
            details={"query": self.context.query, "state": self.workflow_state}
        )
        self._log_step(step)
        
        try:
            if self.workflow_state in self.components:
                # Validate state transition
                if self.workflow_state in self.state_validators:
                    validation_result = self.state_validators[self.workflow_state]()
                    if not validation_result.get("valid", False):
                        return ReasoningStep(
                            agent="Coordinator",
                            operation="validation_failed",
                            details=validation_result
                        )

                # Execute component with retry
                for attempt in range(self.max_retries):
                    try:
                        result = await self.components[self.workflow_state]()
                        if result.operation == "clarification_needed":
                            self.workflow_state = "ANALYZE"  # Retry after clarification
                        elif result.operation != "error":
                            self._advance_state()
                        return result
                    except Exception as e:
                        if attempt == self.max_retries - 1:
                            raise
                        self.logger.warning(f"Attempt {attempt + 1} failed, retrying...")

            raise ValueError(f"Unknown workflow state: {self.workflow_state}")
        except Exception as e:
            self.logger.error(f"Workflow failed: {str(e)}")
            return ReasoningStep(
                agent="Coordinator",
                operation="error_handling",
                details={"error": str(e), "state": self.workflow_state}
            )

    def _advance_state(self):
        states = ["ANALYZE", "EXTRACT", "TRANSFORM", "VISUALIZE"]
        current_idx = states.index(self.workflow_state)
        if current_idx < len(states) - 1:
            self.workflow_state = states[current_idx + 1]

    async def _analyze_phase(self) -> ReasoningStep:
        from .metadata_agent import MetadataAgent
        agent = MetadataAgent(self.context)
            
        result = await agent.execute()
        self.context = self.context.update({"metadata": result.details if result.operation != "error" else {}})
        return result

    async def _extract_phase(self) -> ReasoningStep:
        from .extractor_agent import ExtractorAgent
        agent = ExtractorAgent(self.context)
        result = await agent.execute()
        self.context = self.context.update({"raw_data": result.details if result.operation != "error" else None})
        return result
    
    async def _transform_phase(self) -> ReasoningStep:
        #placeholde for trasnform agent
        return
    
    async def _visualize_phase(self) -> ReasoningStep:
        # Placeholder for GraphAgent
        return ReasoningStep(
            agent="GraphAgent",
            operation="visualize_complete",
            details={"graph_type": "bar", "data": "rental growth"},
            output=self.context.transformed_data
        )

    def _validate_metadata(self) -> Dict[str, Any]:
        if not self.context.metadata:
            return {"valid": False, "reason": "Missing metadata"}
        return {"valid": True}

    def _validate_raw_data(self) -> Dict[str, Any]:
        if self.context.raw_data is None:
            return {"valid": False, "reason": "Missing raw data"}
        return {"valid": True}

    def _validate_transformed_data(self) -> Dict[str, Any]:
        if self.context.transformed_data is None:
            return {"valid": False, "reason": "Missing transformed data"}
        return {"valid": True}

from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import pandas as pd
import logging
from src.agents.context import ContextProtocol

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
    def __init__(self, context: 'ContextProtocol', use_deepseek: bool = False):
        super().__init__(context)
        self.workflow_state = "ANALYZE"
        self.components = {
            "ANALYZE": self._analyze_phase,
            # Add more phases later as needed
        }
        self.state_validators = {
            "EXTRACT": self._validate_metadata,
            # Add more validators for other phases as needed
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
                # Validate state transition if not in ANALYZE phase
                if self.workflow_state != "ANALYZE":
                    validator = self.state_validators.get(self.workflow_state)
                    if validator and not validator():
                        return ReasoningStep(
                            agent="Coordinator",
                            operation="validation_failed",
                            details={
                                "error": f"Validation failed for state {self.workflow_state}",
                                "state": self.workflow_state
                            }
                        )

                result = await self.components[self.workflow_state]()
                
                # Only advance state if operation completed successfully
                # and wasn't a clarification request
                if result.operation not in ["error", "clarification_needed"]:
                    self._advance_state()
                    
                return result
                
            raise ValueError(f"Unknown workflow state: {self.workflow_state}")
            
        except Exception as e:
            self.logger.error(f"Workflow failed: {str(e)}")
            return ReasoningStep(
                agent="Coordinator",
                operation="error",
                details={"error": str(e)}
            )

    async def _analyze_phase(self) -> ReasoningStep:
        """Execute the analysis phase using metadata agent"""
        from .metadata_agent import MetadataAgent
        
        # Pass clarifications from context to the agent
        clarifications = self.context.get("clarifications")
        agent = MetadataAgent(self.context, clarifications=clarifications)
            
        result = await agent.execute() # ReasoningStep from MetadataAgent
        
        # Store metadata analysis in context
        if result.operation == "metadata_analysis":
            # The analysis result *is* the details dictionary
            metadata_analysis = result.details 
            self.context = self.context.update({"metadata": metadata_analysis})
            
            # Check for unresolved ambiguities in the analysis
            if metadata_analysis.get("ambiguities"):
                self.logger.debug(f"Ambiguities found in analysis: {metadata_analysis['ambiguities']}")
                self.logger.debug(f"Current clarifications in context: {self.context.get('clarifications')}")
                # Return clarification step with the ambiguities
                # Ensure we don't proceed if clarifications are needed but not provided yet
                clarifications_exist = bool(self.context.get("clarifications")) # Explicit boolean check
                self.logger.debug(f"Do clarifications exist in context? {clarifications_exist}")
                if not clarifications_exist:
                    self.logger.info("Ambiguities found AND no prior clarifications, requesting clarification.")
                    return ReasoningStep(
                        agent="Coordinator",
                        operation="clarification_needed",
                        details={
                            "ambiguities": metadata_analysis["ambiguities"],
                            "current_state": self.workflow_state
                        }
                    )
                else:
                    # Clarifications were provided, but ambiguities might still exist if LLM couldn't resolve them
                    # Log this, but let the process continue for now. Further logic might be needed.
                    self.logger.warning("Ambiguities found BUT clarifications already exist in context. Proceeding.")
            else:
                self.logger.debug("No ambiguities found in metadata analysis.")
            
            # If no ambiguities requiring clarification, return the original result
            return result 
            
        elif result.operation == "error":
             # Log and return the error step
            self.logger.error(f"MetadataAgent returned error: {result.details}")
            return result
        
        # Handle unexpected operations if necessary
        self.logger.warning(f"MetadataAgent returned unexpected operation: {result.operation}")
        return result

    def _validate_metadata(self) -> bool:
        """Validate metadata analysis before proceeding to extract phase"""
        metadata = self.context.get("metadata", {})
        
        # Check if we have required metadata components
        required_keys = ["tables", "columns", "joins", "aggregations"]
        if not all(key in metadata for key in required_keys):
            return False
            
        # Ensure no unresolved ambiguities
        if metadata.get("ambiguities") and not self.context.get("clarifications"):
            return False
            
        return True

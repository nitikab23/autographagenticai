from abc import ABC, abstractmethod
from typing import Optional, Dict
import logging
from src.agents.context import ContextProtocol

class ReasoningStep:
    def __init__(self, agent: str, operation: str, details: Dict, output: Optional[dict] = None):
        self.agent = agent
        self.operation = operation
        self.details = details
        self.output = output

    def __repr__(self):
        return f"ReasoningStep(agent='{self.agent}', operation='{self.operation}', details={self.details})"

class Agent(ABC):
    def __init__(self, context: ContextProtocol):
        self.context = context
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    async def execute(self) -> ReasoningStep:
        pass

    def _log_step(self, step: ReasoningStep) -> None:
        self.context.log_step(self.__class__.__name__, step.operation, step.details)
        self.logger.debug(f"Performed {step.operation}: {step.details}")
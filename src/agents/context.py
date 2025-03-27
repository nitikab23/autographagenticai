from dataclasses import dataclass
from typing import Dict, Any, List, Optional
import json
from datetime import datetime

@dataclass
class ContextProtocol:
    query: str
    project_id: str
    version: int = 0
    metadata: Dict[str, Any] = None
    raw_data: Optional[Any] = None
    transformed_data: Optional[Any] = None
    reasoning_steps: List[Dict[str, Any]] = None
    ambiguities: List[str] = None
    clarifications: Dict[str, str] = None

    def __post_init__(self):
        self.metadata = self.metadata or {}
        self.reasoning_steps = self.reasoning_steps or []
        self.ambiguities = self.ambiguities or []
        self.clarifications = self.clarifications or {}

    def store(self, key: str, value: Any) -> None:
        setattr(self, key, value)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def log_error(self, error: str) -> None:
        self.log_step("ErrorHandler", "log_error", {"error": error})

    def add_clarifications(self, ambiguities: List[str]) -> None:
        self.ambiguities = ambiguities

    def update(self, updates: Dict[str, Any]) -> "ContextProtocol":
        return ContextProtocol(
            query=self.query,
            project_id=self.project_id,
            version=self.version + 1,
            metadata=updates.get("metadata", self.metadata),
            raw_data=updates.get("raw_data", self.raw_data),
            transformed_data=updates.get("transformed_data", self.transformed_data),
            reasoning_steps=updates.get("reasoning_steps", self.reasoning_steps),
            ambiguities=updates.get("ambiguities", self.ambiguities),
            clarifications=updates.get("clarifications", self.clarifications)
        )

    def log_step(self, agent: str, action: str, details: Dict[str, Any]):
        self.reasoning_steps.append({
            "agent": agent,
            "action": action,
            "details": details,
            "timestamp": datetime.utcnow().isoformat()
        })

    def snapshot(self) -> Dict:
        return json.loads(self.to_json())

    def to_json(self) -> str:
        return json.dumps(
            {k: v for k, v in self.__dict__.items() if v is not None},
            default=str
        )
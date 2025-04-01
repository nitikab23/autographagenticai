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
    reasoning_steps: List[Dict[str, Any]] = None
    clarifications: Dict[str, str] = None

    def __post_init__(self):
        self.metadata = self.metadata or {}
        self.reasoning_steps = self.reasoning_steps or []
        self.clarifications = self.clarifications or {}

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def update(self, updates: Dict[str, Any]) -> "ContextProtocol":
        return ContextProtocol(
            query=self.query,
            project_id=self.project_id,
            version=self.version + 1,
            metadata=updates.get("metadata", self.metadata),
            reasoning_steps=updates.get("reasoning_steps", self.reasoning_steps),
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
        return {
            "query": self.query,
            "project_id": self.project_id,
            "version": self.version,
            "metadata": self.metadata,
            "reasoning_steps": self.reasoning_steps,
            "clarifications": self.clarifications
        }
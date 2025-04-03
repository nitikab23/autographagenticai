from dataclasses import dataclass, field # Import field
from typing import Dict, Any, List, Optional
import json
from datetime import datetime
import copy # Import copy for deep copies

# Use standard dataclass without slots for flexibility
@dataclass
class ContextProtocol:
    query: str
    project_id: str
    version: int = 0
    # Use field to initialize mutable defaults correctly
    _data: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        # Ensure standard keys exist even if not provided initially
        # These will be stored within _data now
        self._data.setdefault("metadata", {})
        self._data.setdefault("reasoning_steps", [])
        self._data.setdefault("clarifications", {})
        # Store initial required fields also in _data for consistency
        self._data["query"] = self.query
        self._data["project_id"] = self.project_id
        self._data["version"] = self.version


    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from the context data."""
        # Prioritize explicit attributes if they exist (like query, project_id, version)
        # though they are also in _data now.
        if hasattr(self, key) and key not in ['_data']:
             # This check might become redundant if we fully rely on _data
             # but keeps compatibility if direct attributes are accessed elsewhere.
             return getattr(self, key, default)
        # Fallback to checking the internal data dictionary
        return self._data.get(key, default)

    def update(self, updates: Dict[str, Any]) -> "ContextProtocol":
        """Creates a new context instance with updated values."""
        new_data = copy.deepcopy(self._data) # Deep copy to avoid modifying old context
        new_data.update(updates) # Merge updates into the copied data

        # Ensure core fields are updated if present in 'updates'
        new_query = updates.get("query", self.query)
        new_project_id = updates.get("project_id", self.project_id)
        new_version = self.version + 1 # Always increment version
        new_data["version"] = new_version # Update version in data dict too

        # Create the new instance, passing only the required positional args
        # and the merged data dictionary for **kwargs handling if needed,
        # but here we'll rely on __post_init__ using the new_data.
        # A simpler approach might be needed if __init__ needs **kwargs directly.
        # Let's try passing the whole new_data dict.

        # Re-create the object, passing required args and the updated data dict
        # We need to ensure the __init__ or __post_init__ correctly uses this _data
        new_context = ContextProtocol(
            query=new_query,
            project_id=new_project_id,
            version=new_version,
             # Pass the fully merged data dictionary
            _data=new_data
        )
        # Ensure post_init logic runs correctly if needed, though passing _data might bypass it.
        # Let's refine: Pass only required args, let __post_init__ handle _data setup, then update.
        new_context = ContextProtocol(
             query=new_query,
             project_id=new_project_id,
             version=new_version
             # Let __post_init__ create default _data
        )
        # Now explicitly set the merged data onto the new instance
        new_context._data = new_data

        return new_context


    def log_step(self, agent: str, action: str, details: Dict[str, Any]):
        """Logs a reasoning step into the context's data."""
        # Ensure reasoning_steps list exists
        if "reasoning_steps" not in self._data or not isinstance(self._data["reasoning_steps"], list):
            self._data["reasoning_steps"] = []
        self._data["reasoning_steps"].append({
            "agent": agent,
            "action": action,
            "details": details,
            "timestamp": datetime.utcnow().isoformat()
        })

    def snapshot(self) -> Dict:
        """Return a dictionary representation of the context."""
        # Return a copy of the internal data dictionary
        return copy.deepcopy(self._data)

    # Allow direct attribute access for backward compatibility if needed,
    # but prefer using get() method.
    def __getattr__(self, name: str) -> Any:
        """Allow getting items from _data via attribute access."""
        if name == '_data':
            # Prevent recursion if _data itself is accessed before initialization
             raise AttributeError("'_data' not initialized yet")
        if name in self._data:
            return self._data[name]
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    # Optional: Allow setting attributes directly into _data
    # def __setattr__(self, name: str, value: Any) -> None:
    #     if name in ['query', 'project_id', 'version', '_data']:
    #         super().__setattr__(name, value)
    #     else:
    #         self._data[name] = value

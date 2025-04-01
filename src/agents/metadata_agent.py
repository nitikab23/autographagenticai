from typing import Dict, Optional
import json
import logging
import os
from google import genai
from src.agents.core import Agent, ReasoningStep
from src.agents.context import ContextProtocol
from src.project_management.project_manager import ProjectManager
from dotenv import load_dotenv

class MetadataAgent(Agent):
    def __init__(self, context: ContextProtocol, clarifications: Optional[Dict[str, str]] = None):
        super().__init__(context)
        self.clarifications = clarifications or context.get("clarifications", {})
        load_dotenv()
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY not set")
        self.client = genai.Client(api_key=api_key)
        self.project_manager = ProjectManager(storage_path="storage/metadata/")
        self._load_prompts()

    async def execute(self) -> ReasoningStep:
        try:
            metadata = self.project_manager.get_project_metadata(self.context.project_id)
            if not metadata.get("tables"):
                return ReasoningStep(
                    agent="MetadataAgent",
                    operation="error",
                    details={"error": f"No metadata for project {self.context.project_id}"}
                )
            result = await self.analyze(self.context.query, metadata)
            return ReasoningStep(
                agent="MetadataAgent",
                operation="metadata_analysis",
                details=result
            )
        except Exception as e:
            self.logger.error(f"Analysis failed: {str(e)}")
            return ReasoningStep(
                agent="MetadataAgent",
                operation="error",
                details={"error": str(e)}
            )

    async def analyze(self, query: str, metadata: Dict) -> Dict:
        metadata_str = json.dumps(metadata, indent=2)
        context_str = json.dumps(self.context.snapshot(), indent=2)
        clarifications_str = "No clarifications provided." if not self.clarifications else "\n".join(
            f"- {q}: {a}" for q, a in self.clarifications.items()
        )
        prompt = self.analyze_prompt.format(
            query=query,
            metadata=metadata_str,
            context=context_str,
            clarifications=clarifications_str
        )
        self.logger.debug(f"Generated prompt:\n{prompt}")
        response = self.client.models.generate_content(
            model="gemini-2.5-pro-exp-03-25",
            contents=[prompt]
        )
        content = response.candidates[0].content.parts[0].text.strip("```json\n")
        result = json.loads(content)

        # Minimal validation and cleanup
        if "ambiguities" in result and self.clarifications:
            result["ambiguities"] = [
                amb for amb in result["ambiguities"]
                if amb["question"] not in self.clarifications
            ]
        self._validate_result(result, metadata)

        return result

    def _validate_result(self, result: Dict, metadata: Dict):
        # Use inner structure if "tables" exists; otherwise, use metadata directly.
        meta = metadata.get("tables", metadata)
        all_meta_cols = {
            f"{table}.{col['name']}": col
            for table, table_meta in meta.items()
            for col in table_meta.get("columns", [])
        }
        for table, cols in result["columns"].items():
            for col in cols:
                full_name = f"{table}.{col['name']}"
                if full_name not in all_meta_cols:
                    self.logger.warning(f"Column {full_name} not in metadata, removing")
                    cols.remove(col)
                elif col != all_meta_cols[full_name]:
                    cols[cols.index(col)] = all_meta_cols[full_name]

        # Check drill_down and filters
        for level in result["drill_down"]["levels"]:
            for col in level["columns"]:
                if " AS " in col or "||" in col:
                    continue  # Allow expressions
                base_col = col.split(".")[-1]
                table = ".".join(col.split(".")[:-1])
                if col not in all_meta_cols and not any(c["name"] == base_col for c in result["columns"].get(table, [])):
                    self.logger.warning(f"Drill-down column {col} not in columns, adding")
                    if col in all_meta_cols:
                        result["columns"].setdefault(table, []).append(all_meta_cols[col])

        for filt in result["filters"]:
            col = filt["column"]
            if col not in all_meta_cols:
                self.logger.warning(f"Filter column {col} not in metadata, removing")
                result["filters"].remove(filt)

    def _load_prompts(self):
        try:
            with open("src/agents/prompts/metadata/analyze_query.txt", "r", encoding="utf-8") as f:
                self.analyze_prompt = f.read().strip()
            self.logger.debug("Prompts loaded successfully")
        except Exception as e:
            self.logger.error(f"Failed to load prompts: {str(e)}")
            raise
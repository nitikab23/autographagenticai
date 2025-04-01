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
            self.logger.debug(f"Raw metadata from project manager: {json.dumps(metadata, indent=2)}")
            if not metadata.get("tables") and not metadata.get("columns"):
                self.logger.warning("Project metadata is empty, relying on LLM output")
            result = await self.analyze(self.context.query, metadata)
            self.logger.debug(f"Post-analysis result: {json.dumps(result, indent=2)}")
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
        response = self.client.models.generate_content(
            model="gemini-2.5-pro-exp-03-25",
            contents=[prompt]
        )
        self.logger.debug(f"Response: {response}")
        content = response.candidates[0].content.parts[0].text.strip("```json\n")
        result = json.loads(content)
        self.logger.debug(f"Result: {result}")

        # Minimal validation and cleanup
        if "ambiguities" in result and self.clarifications:
            result["ambiguities"] = [
                amb for amb in result["ambiguities"]
                if amb["question"] not in self.clarifications
            ]
        self._validate_result(result, metadata)

        return result

    def _validate_result(self, result: Dict, metadata: Dict):
        # Ensure all expected keys are present
        expected_keys = {"tables", "columns", "joins", "filters", "drill_down", "ambiguities"}
        for key in expected_keys:
            if key not in result:
                result[key] = [] if key != "columns" else {}
                if key == "drill_down":
                    result[key] = {"levels": []}

        # Get metadata columns if available
        all_meta_cols = {
            f"{table}.{col['name']}": col
            for table, cols in metadata.get("columns", {}).items()
            for col in cols
        }
        all_meta_tables = set(metadata.get("tables", []))
        self.logger.debug(f"Metadata columns: {list(all_meta_cols.keys())}")

        # Validate columns, preserving LLM output unless overridden by metadata
        for table in list(result["columns"].keys()):
            if all_meta_tables and table not in all_meta_tables:
                self.logger.warning(f"Table {table} not in metadata.tables, removing")
                del result["columns"][table]
                continue
            cols = result["columns"][table]
            validated_cols = []
            for col in cols:
                full_name = f"{table}.{col['name']}"
                if full_name in all_meta_cols:
                    validated_cols.append(all_meta_cols[full_name])  # Prefer metadata version
                    self.logger.debug(f"Using metadata column for {full_name}")
                else:
                    validated_cols.append(col)  # Keep LLM version if metadata lacks it
                    self.logger.debug(f"Keeping LLM column {full_name}")
            result["columns"][table] = validated_cols

        # Validate joins, preserving LLM output unless invalid
        validated_joins = []
        for join in result["joins"]:
            if not isinstance(join, dict) or not all(k in join for k in ["left_table", "left_column", "right_table", "right_column"]):
                self.logger.warning(f"Invalid join format: {join}, skipping")
                continue
            left_col = join["left_column"].split(".")[-1] if "." in join["left_column"] else join["left_column"]
            right_col = join["right_column"].split(".")[-1] if "." in join["right_column"] else join["right_column"]
            left_full_col = f"{join['left_table']}.{left_col}"
            right_full_col = f"{join['right_table']}.{right_col}"
            # Only skip if metadata exists and join is completely invalid
            if all_meta_cols and (
                join["left_table"] not in all_meta_tables or
                join["right_table"] not in all_meta_tables or
                (left_full_col not in all_meta_cols and right_full_col not in all_meta_cols)
            ):
                self.logger.warning(f"Join columns {left_full_col} or {right_full_col} not in metadata, skipping")
                continue
            join_type = join.get("type", "LEFT")  # Default to LEFT, respect LLM type if provided
            validated_joins.append({
                "left_table": join["left_table"],
                "left_column": left_col,
                "right_table": join["right_table"],
                "right_column": right_col,
                "type": join_type
            })
        result["joins"] = validated_joins

        # Validate drill_down, preserving expressions and columns
        for level in result["drill_down"]["levels"]:
            validated_cols = []
            for col in level["columns"]:
                if " AS " in col or "||" in col or "CONCAT" in col:  # Allow expressions
                    validated_cols.append(col)
                    continue
                table = ".".join(col.split(".")[:-1])
                base_col = col.split(".")[-1]
                full_name = f"{table}.{base_col}"
                if all_meta_cols and full_name in all_meta_cols:
                    validated_cols.append(col)
                    if not any(c["name"] == base_col for c in result["columns"].get(table, [])):
                        self.logger.debug(f"Adding drill-down column {full_name} to columns")
                        result["columns"].setdefault(table, []).append(all_meta_cols[full_name])
                else:
                    validated_cols.append(col)  # Keep LLM version
                    self.logger.debug(f"Keeping drill-down column {full_name}")
            level["columns"] = validated_cols

        # Validate filters
        result["filters"] = [
            filt for filt in result["filters"]
            if isinstance(filt, dict) and (not all_meta_cols or filt.get("column") in all_meta_cols)
        ]

        # Ensure tables align only if metadata.tables exists
        if all_meta_tables:
            result["tables"] = [t for t in result["tables"] if t in all_meta_tables]

    def _load_prompts(self):
        try:
            with open("src/agents/prompts/metadata/analyze_query.txt", "r", encoding="utf-8") as f:
                self.analyze_prompt = f.read().strip()
            self.logger.debug("Prompts loaded successfully")
        except Exception as e:
            self.logger.error(f"Failed to load prompts: {str(e)}")
            raise
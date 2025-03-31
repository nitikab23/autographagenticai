from typing import Dict, Any, Optional
import json
import logging
import os
from tenacity import retry, stop_after_attempt, wait_exponential
from openai import OpenAI
from src.agents.core import Agent, ReasoningStep
from src.agents.context import ContextProtocol
from src.project_management.project_manager import ProjectManager
from google import genai
from google.genai import types
from dotenv import load_dotenv

class MetadataAgent(Agent):
    """LLM-driven metadata analysis with context-aware processing"""
    
    def __init__(self, context: ContextProtocol, clarifications: Optional[Dict[str, str]] = None):
        super().__init__(context)
        self.clarifications = clarifications
        load_dotenv()
        
        gemini_api_key = os.getenv('GEMINI_API_KEY')
        if not gemini_api_key:
            raise ValueError("GEMINI_API_KEY environment variable is not set")
            
        from src.project_management.project_manager import ProjectManager
        self.project_manager = ProjectManager(
            storage_path="storage/metadata/"
        )
        self.client = genai.Client(
            api_key=gemini_api_key,
        )
        self._load_prompts()

    async def execute(self) -> ReasoningStep:
        """Execute metadata analysis"""
        try:
            metadata = self.project_manager.get_project_metadata(self.context.project_id)
            if not metadata.get('tables'):
                return ReasoningStep(
                    agent="MetadataAgent",
                    operation="error",
                    details={"error": f"No metadata found for project {self.context.project_id}"}
                )

            result = await self.analyze(
                query=self.context.query,
                project_id=self.context.project_id,
                context=self.context,
                raw_metadata=metadata,
                clarifications=self.clarifications # Pass clarifications here
            )

            return ReasoningStep(
                agent="MetadataAgent",
                operation="metadata_analysis",
                details=result
            )

        except Exception as e:
            self.logger.error(f"Metadata analysis failed: {str(e)}")
            return ReasoningStep(
                agent="MetadataAgent",
                operation="error",
                details={"error": str(e)}
            )

    async def analyze(self, query: str, project_id: str, context: ContextProtocol, raw_metadata: Dict, clarifications: Optional[Dict[str, str]] = None) -> Dict:
        """Analyze metadata using LLM, incorporating user clarifications"""
        try:
            metadata_str = json.dumps(raw_metadata, indent=2)
            context_str = json.dumps(context.to_json(), indent=2)
            
            # Format clarifications for the prompt
            clarifications_str = "No clarifications provided."
            if clarifications:
                clarifications_list = [f"- {q}: {a}" for q, a in clarifications.items()]
                clarifications_str = "User provided the following clarifications:\n" + "\n".join(clarifications_list)

            formatted_prompt = self.analyze_prompt.format(
                query=query,
                metadata=metadata_str,
                context=context_str,
                clarifications=clarifications_str # Add clarifications to prompt formatting
            )
            response = self.client.models.generate_content(
                model="gemini-2.5-pro-exp-03-25",
                contents=[formatted_prompt]
            )
            self.logger.debug(f"Raw API Response: {response}")
            content = response.candidates[0].content.parts[0].text
            content = content.replace("```json", "").replace("```", "").strip()
            
            # Parse to dict for consistency
            return json.loads(content)
        except Exception as e:
            self.logger.error(f"Metadata analysis failed: {str(e)}")
            raise

    def _load_prompts(self):
        """Load prompt templates"""
        try:
            with open("src/agents/prompts/metadata/analyze_query.txt", encoding='utf-8') as f:
                self.analyze_prompt = f.read().strip()
            with open("src/agents/prompts/metadata/discover_relationships.txt", encoding='utf-8') as f:
                self.relationship_prompt = f.read().strip()
            
            # Validate that the prompt contains the expected placeholders
            # Update validation to include the new clarifications placeholder
            test_format = self.analyze_prompt.format(
                query="test",
                metadata={},
                context="{}",
                clarifications="test clarifications"
            )
            self.logger.debug("Successfully loaded and validated prompt templates")
            
        except Exception as e:
            self.logger.error(f"Failed to load prompt templates: {str(e)}")
            raise

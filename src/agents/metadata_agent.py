from typing import Dict, Any
import json
import logging
from tenacity import retry, stop_after_attempt, wait_exponential
from openai import OpenAI
from src.agents.core import Agent, ReasoningStep
from src.agents.context import ContextProtocol
from src.metadata_management.metadata_store import MetadataStore

class MetadataAgent(Agent):
    """LLM-driven metadata analysis with context-aware processing"""
    
    def __init__(self, context: ContextProtocol):
        super().__init__(context)
        from src.project_management.project_manager import ProjectManager
        self.metadata_store = MetadataStore(
            storage_path="storage/metadata/"
        )
        self.client = OpenAI()
        self._load_prompts()

    async def execute(self) -> ReasoningStep:
        """Execute metadata analysis"""
        try:
            metadata = self.metadata_store.get_project_metadata(self.context.project_id)
            if not metadata:
                return ReasoningStep(
                    agent="MetadataAgent",
                    operation="error",
                    details={"error": f"No metadata found for project {self.context.project_id}"}
                )

            result = await self.analyze(
                query=self.context.query,
                project_id=self.context.project_id,
                context=self.context,
                raw_metadata=metadata
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

    async def analyze(self, query: str, project_id: str, context: ContextProtocol, raw_metadata: Dict) -> Dict:
        """Analyze metadata using LLM"""
        try:
            # Convert metadata and context to strings to avoid JSON formatting issues
            metadata_str = json.dumps(raw_metadata, indent=2)
            context_str = json.dumps(context.to_json(), indent=2)
            
            # Format the prompt template with the actual values
            formatted_prompt = self.analyze_prompt.format(
                query=query,
                metadata=metadata_str,
                context=context_str
            )

            # Execute the prompt with OpenAI using synchronous API
            response = self.client.chat.completions.create(
                model="gpt-4o-mini-2024-07-18",
                messages=[
                    {"role": "system", "content": "You are a data analysis assistant. Always respond with valid JSON."},
                    {"role": "user", "content": formatted_prompt}
                ],
                temperature=0.3,
                max_tokens=1000
            )

            # Extract the response content
            result = response.choices[0].message.content
            return json.loads(result)

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
            test_format = self.analyze_prompt.format(
                query="test",
                metadata={},
                context="{}"
            )
            self.logger.debug("Successfully loaded and validated prompt templates")
            
        except Exception as e:
            self.logger.error(f"Failed to load prompt templates: {str(e)}")
            raise

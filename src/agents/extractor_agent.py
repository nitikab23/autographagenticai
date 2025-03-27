from src.agents.context import ContextProtocol
from src.agents.core import Agent, ReasoningStep
from src.core.service_layer import llm_service
import logging
import pandas as pd
from typing import Dict, Any
import json

class ExtractorAgent(Agent):
    """Handles SQL generation and data extraction with full visibility"""
    
    def __init__(self, context: 'ContextProtocol'):
        super().__init__(context)
        self._load_prompts()

    def execute(self) -> ReasoningStep:
        """Execute the data extraction workflow"""
        step = ReasoningStep(
            agent="ExtractorAgent",
            operation="extraction_start",
            details={"query": self.context.query}
        )
        self._log_step(step)
        
        try:
            # 1. Generate SQL
            sql_step = self._generate_sql()
            if sql_step.details.get('status') != 'success':
                return sql_step

            # 2. Execute query
            execution_step = self._execute_query(sql_step.details['generated_sql'])
            
            return ReasoningStep(
                agent="ExtractorAgent",
                operation="extraction_complete",
                details={
                    "row_count": len(self.context.raw_data),
                    "sample_data": self.context.raw_data.head(3).to_dict()
                }
            )
            
        except Exception as e:
            self.logger.error(f"Data extraction failed: {str(e)}")
            return ReasoningStep(
                agent="ExtractorAgent",
                operation="error",
                details={"error": str(e)}
            )

    def _generate_sql(self) -> ReasoningStep:
        """Generate SQL using LLM with metadata context"""
        response = llm_service.execute_prompt_sync(
            self.sql_generation_prompt,
            query=self.context.query,
            metadata=json.dumps(self.context.metadata, indent=2)
        )
        
        try:
            sql_result = json.loads(response)
            step = ReasoningStep(
                agent="ExtractorAgent",
                operation="sql_generation",
                details={
                    "generated_sql": sql_result.get("query"),
                    "confidence": sql_result.get("confidence_score"),
                    "validation_errors": sql_result.get("validation_errors", [])
                }
            )
            
            if not sql_result.get("query") or sql_result.get("validation_errors"):
                step.details['status'] = 'failed'
                step.details['error'] = 'Invalid SQL generated'
                return step
                
            self.context.generated_sql = sql_result["query"]
            step.details['status'] = 'success'
            return step
            
        except json.JSONDecodeError as e:
            return ReasoningStep(
                agent="ExtractorAgent",
                operation="error",
                details={"error": f"LLM response parsing failed: {str(e)}"}
            )

    def _execute_query(self, sql: str) -> ReasoningStep:
        """Execute SQL against the database"""
        # Actual execution logic would go here
        # For now mock with sample data
        self.context.raw_data = pd.DataFrame({
            'film_id': [1, 2, 3],
            'title': ['Movie A', 'Movie B', 'Movie C'],
            'rental_count': [45, 32, 28]
        })
        
        return ReasoningStep(
            agent="ExtractorAgent",
            operation="query_execution",
            details={
                "executed_sql": sql,
                "row_count": len(self.context.raw_data),
                "status": "success"
            }
        )

    def _load_prompts(self):
        """Load SQL generation prompt template"""
        with open("src/agents/prompts/extractor/sql_generation.txt") as f:
            self.sql_generation_prompt = f.read().strip()

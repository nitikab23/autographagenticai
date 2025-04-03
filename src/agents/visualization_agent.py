import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import re # Added for parsing LLM response
import io # Added for capturing df.info() output
from typing import Dict, Any
from src.agents.core import Agent, ReasoningStep
from src.agents.context import ContextProtocol
from google import genai
from dotenv import load_dotenv # Added dotenv

# Load environment variables
load_dotenv()

class VisualizationAgent(Agent):
    def __init__(self, context: ContextProtocol):
        super().__init__(context)
        # Initialize the Gemini model (adjust model name if needed)
        # Consider making the model name configurable
        try:
             # Ensure API key is configured before initializing
             api_key = os.getenv("GEMINI_API_KEY")
             if not api_key:
                raise ValueError("GEMINI_API_KEY not set")
             self.client = genai.Client(api_key=api_key)
             self.logger.info(f"Initialized Genai Client")
        except Exception as e:
             self.logger.error(f"Failed to initialize Genai Client {e}", exc_info=True)
             # Decide how to handle this - maybe raise the error or set model to None
             # For now, let the error propagate if initialization fails.
             raise

    async def execute(self) -> ReasoningStep:
        self.logger.info("VisualizationAgent starting execution.")
        try:
            # 1. Get data path from context
            query_result_path = self.context.get("query_result_path")
            if not query_result_path or not os.path.exists(query_result_path):
                error_msg = f"Input CSV file path not found or invalid in context: {query_result_path}"
                self.logger.error(error_msg)
                return ReasoningStep(agent="VisualizationAgent", operation="error", details={"error": error_msg})

            self.logger.info(f"Reading data from: {query_result_path}")
            df = pd.read_csv(query_result_path)

            # Get the query-specific output directory from context
            query_output_dir = self.context.get("query_output_dir")
            if not query_output_dir or not os.path.isdir(query_output_dir):
                 # Fallback or error if directory not provided/found
                 self.logger.warning(f"Query output directory not found in context ('{query_output_dir}'). Saving visualization to default location.")
                 # Define a fallback path if needed, e.g., storage/visualizations
                 # For now, let's raise an error if the specific dir isn't passed correctly
                 error_msg = f"Query-specific output directory not found or invalid in context: {query_output_dir}"
                 self.logger.error(error_msg)
                 return ReasoningStep(agent="VisualizationAgent", operation="error", details={"error": error_msg})

            # Define visualization path inside the query-specific directory
            output_image_path = os.path.join(query_output_dir, "visualization.png") # Consistent filename
            self.logger.info(f"Target output image path: {output_image_path}")

            # 2. Prepare data summary for LLM
            data_head = df.head().to_string()
            # Capture df.info() output into a string
            buf = io.StringIO()
            df.info(buf=buf, verbose=True)
            data_info_str = buf.getvalue()

            # Get user query from context
            user_query = self.context.get("query", "Generate an appropriate visualization.") # Default query if none provided

            # 3. Load and format prompt for LLM
            try:
                prompt_template = self._load_prompt_template()
                prompt = prompt_template.format(
                    user_query=user_query,
                    data_head=data_head,
                    data_info=data_info_str,
                    output_image_path=output_image_path
                )
                self.logger.debug(f"Formatted LLM Prompt:\n{prompt}")
            except Exception as e:
                 error_msg = f"Failed to load or format prompt template: {e}"
                 self.logger.error(error_msg, exc_info=True)
                 return ReasoningStep(agent="VisualizationAgent", operation="error", details={"error": error_msg})


            # 4. Call LLM to generate visualization code
            try:
                self.logger.info("Calling LLM to generate visualization code...")
                # Use generate_content for Gemini API
                response = self.client.models.generate_content(
            model="gemini-2.5-pro-exp-03-25",
            contents=[prompt]
        )
                # TODO: Add more robust error handling for API responses (e.g., check safety ratings, finish reason)
                # Access response text correctly based on Gemini API structure
                matplotlib_code = response.candidates[0].content.parts[0].text
                self.logger.debug(f"Raw LLM Output:\n{matplotlib_code}")

                # 5. Parse the generated code from the LLM response
                generated_code = self._parse_code_from_response(matplotlib_code)
                if not generated_code:
                     error_msg = "Failed to parse Python code block from LLM response."
                     self.logger.error(f"{error_msg} Raw response was:\n{matplotlib_code}")
                     return ReasoningStep(agent="VisualizationAgent", operation="error", details={"error": error_msg, "llm_response": matplotlib_code})
                self.logger.debug(f"Parsed Generated Code:\n{generated_code}")

            except Exception as e:
                 error_msg = f"LLM call failed: {str(e)}"
                 self.logger.error(error_msg, exc_info=True)
                 return ReasoningStep(agent="VisualizationAgent", operation="error", details={"error": error_msg})


            # 6. Execute the generated code
            self.logger.info("Executing LLM-generated visualization code...")
            # IMPORTANT: exec() is dangerous. Ensure the LLM is trusted or sandbox the execution.
            # Provide necessary context (df, plt, sns, output_image_path) to the exec environment
            exec_globals = {
                'pd': pd, # Provide pandas
                'plt': plt,
                'sns': sns,
                'df': df,
                'output_image_path': output_image_path # Provide the specific output path
            }
            try:
                 exec(generated_code, exec_globals)
                 self.logger.info(f"Visualization saved to {output_image_path}")
            except Exception as e:
                 error_msg = f"Execution of LLM-generated code failed: {str(e)}"
                 self.logger.error(error_msg, exc_info=True)
                 # Include generated code in error details for debugging
                 return ReasoningStep(agent="VisualizationAgent", operation="error", details={"error": error_msg, "generated_code": generated_code})


            # 7. Return success
            result = ReasoningStep(
                agent="VisualizationAgent",
                operation="visualization_generated",
                details={
                    "input_data_path": query_result_path,
                    "output_image_path": output_image_path,
                    "executed_code": generated_code # Include for debugging/transparency
                }
            )
            self._log_step(result)
            return result

        except FileNotFoundError:
             error_msg = f"Input CSV file not found at path: {query_result_path}"
             self.logger.error(error_msg)
             return ReasoningStep(agent="VisualizationAgent", operation="error", details={"error": error_msg})
        except Exception as e:
            self.logger.error(f"Visualization generation failed: {str(e)}", exc_info=True)
            # Log the general error and return an error step
            error_details = {"error": f"Visualization generation failed: {str(e)}"}
            self.logger.error(f"General execution error. Details: {error_details}", exc_info=True)
            return ReasoningStep(
                agent="VisualizationAgent",
                operation="error",
                details=error_details
            )

    def _load_prompt_template(self) -> str:
        """Loads the prompt template from the file. Called during init."""
        # Consider making the path configurable
        prompt_path = "src/agents/prompts/visualization/generate_code.txt"
        try:
            with open(prompt_path, 'r') as f:
                return f.read()
        except FileNotFoundError:
            self.logger.error(f"Prompt template file not found at {prompt_path}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to read prompt template file {prompt_path}: {e}")
            raise

    def _parse_code_from_response(self, response_text: str) -> str | None:
        """Extracts the Python code block from the LLM response."""
        # Regex to find code block enclosed in triple backticks (```python ... ```)
        match = re.search(r"```python\s*([\s\S]+?)\s*```", response_text, re.MULTILINE)
        if match:
            return match.group(1).strip()
        else:
            # Fallback: Maybe the LLM just returned code without backticks?
            # Be cautious with this fallback.
            if "import " in response_text and ("plt." in response_text or "sns." in response_text):
                 self.logger.warning("LLM response did not contain standard Python code block markers (```python). Attempting to use the whole response as code.")
                 return response_text.strip()
            return None # Could not find a code block

import pandas as pd
# import matplotlib.pyplot as plt # Removed
# import seaborn as sns # Removed
import plotly.express as px # Added
import os
import re # Added for parsing LLM response
import io # Added for capturing df.info() output
import uuid # Added for generating valid div IDs
from typing import Dict, Any
from src.agents.core import Agent, ReasoningStep
from src.agents.context import ContextProtocol
from google import genai
from dotenv import load_dotenv # Added dotenv
from google.genai import types
from openai import OpenAI


# Load environment variables
load_dotenv()

class VisualizationAgent(Agent):
    def __init__(self, context: ContextProtocol):
        super().__init__(context)
        # Initialize the Gemini model (adjust model name if needed)
        # Consider making the model name configurable
        try:
             # Ensure API key is configured before initializing
             api_key = os.getenv("DEEPSEEK_API_KEY")
             if not api_key:
                raise ValueError("DEEPSEEK_API_KEY not set")
             self.client = OpenAI(
            api_key=api_key,
            base_url="https://api.deepseek.com"
        )
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
            output_html_path = os.path.join(query_output_dir, "visualization.html") # Changed variable name and extension
            self.logger.info(f"Target output HTML path: {output_html_path}")

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
                    data_info=data_info_str
                    # output_image_path removed as it's not in the new prompt
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
                response = self.client.chat.completions.create(
            model="deepseek-chat",
            messages=[{"role": "user" ,"content" : prompt}],
            temperature=0.2
              )
                # TODO: Add more robust error handling for API responses (e.g., check safety ratings, finish reason)
                # Access response text correctly based on Gemini API structure
                visualization_code = response.choices[0].message.content
                self.logger.debug(f"Raw LLM Output:\n{visualization_code}")

                # 5. Parse the generated code and summary from the LLM response
                generated_code, visualization_summary = self._parse_code_and_summary(visualization_code)

                if not generated_code:
                     error_msg = "Failed to parse Python code block from LLM response."
                     self.logger.error(f"{error_msg} Raw response was:\n{visualization_code}")
                     # Still return error if code is missing, even if summary exists
                     return ReasoningStep(agent="VisualizationAgent", operation="error", details={"error": error_msg, "llm_response": visualization_code})
                
                if not visualization_summary:
                    self.logger.warning(f"Could not parse summary from LLM response. Raw response was:\n{visualization_code}")
                    # Proceed without summary if code is present

                self.logger.debug(f"Parsed Generated Code:\n{generated_code}")
                self.logger.debug(f"Parsed Summary:\n{visualization_summary}")


            except Exception as e:
                 error_msg = f"LLM call failed: {str(e)}"
                 self.logger.error(error_msg, exc_info=True)
                 return ReasoningStep(agent="VisualizationAgent", operation="error", details={"error": error_msg})


            # 6. Execute the generated code
            self.logger.info("Executing LLM-generated visualization code...")
            # IMPORTANT: exec() is dangerous. Ensure the LLM is trusted or sandbox the execution.
            # Provide necessary context (df, px) to the exec environment
            exec_globals = {
                'pd': pd, # Provide pandas
                'px': px, # Provide Plotly Express
                'df': df
                # output_path removed, saving handled after exec
            }
            try:
                 # Execute the code which should define 'fig' in exec_globals
                 exec(generated_code, exec_globals)

                 # Check if 'fig' was created and generate HTML string
                 visualization_html = None # Initialize
                 if 'fig' in exec_globals:
                     fig = exec_globals['fig']
                     # Generate HTML string for embedding, include Plotly.js from CDN
                     # Generate a valid ID prefixed for CSS compatibility
                     valid_div_id = f"plotly-div-{uuid.uuid4()}"
                     visualization_html = fig.to_html(full_html=False, include_plotlyjs='cdn', div_id=valid_div_id)
                     self.logger.info(f"Generated Plotly HTML string with ID: {valid_div_id}")
                 else:
                     error_msg = "LLM generated code did not define a 'fig' variable."
                     self.logger.error(error_msg)
                     return ReasoningStep(agent="VisualizationAgent", operation="error", details={"error": error_msg, "generated_code": generated_code})

            except Exception as e:
                 error_msg = f"Execution of LLM-generated code or saving HTML failed: {str(e)}"
                 self.logger.error(error_msg, exc_info=True)
                 # Include generated code in error details for debugging
                 return ReasoningStep(agent="VisualizationAgent", operation="error", details={"error": error_msg, "generated_code": generated_code})


            # 7. Return success
            result = ReasoningStep(
                agent="VisualizationAgent",
                operation="visualization_generated",
                details={
                    "input_data_path": query_result_path,
                    "visualization_html": visualization_html, # Add HTML string
                    "visualization_summary": visualization_summary, # Add summary string
                    "executed_code": generated_code # Include for debugging/transparency
                    # Removed output_html_path
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

    def _parse_code_and_summary(self, response_text: str) -> tuple[str | None, str | None]:
        """Extracts the Python code block and the summary text from the LLM response."""
        code = None
        summary = None

        # Regex to find code block enclosed in triple backticks (```python ... ```)
        code_match = re.search(r"```python\s*([\s\S]+?)\s*```", response_text, re.MULTILINE)
        if code_match:
            code = code_match.group(1).strip()
            # Look for summary *after* the code block
            summary_marker = "--- SUMMARY ---"
            summary_start_index = response_text.find(summary_marker, code_match.end())
            if summary_start_index != -1:
                summary = response_text[summary_start_index + len(summary_marker):].strip()
        else:
            # Fallback: Maybe the LLM just returned code without backticks?
            # This fallback is less likely to work reliably with the summary format.
            if "import plotly.express as px" in response_text and "px." in response_text:
                 self.logger.warning("LLM response did not contain standard Python code block markers (```python). Attempting to use the whole response as code.")
                 # Attempt to find summary even in fallback
                 summary_marker = "--- SUMMARY ---"
                 summary_start_index = response_text.find(summary_marker)
                 if summary_start_index != -1:
                     # Assume code is everything before the marker
                     code = response_text[:summary_start_index].strip()
                     summary = response_text[summary_start_index + len(summary_marker):].strip()
                 else:
                     # Assume the whole response is code if no marker found
                     code = response_text.strip()

        return code, summary

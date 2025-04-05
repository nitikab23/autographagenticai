import sys
from pathlib import Path
import json
import asyncio
import logging
import os # Added for environment variables
from dotenv import load_dotenv # Added for .env loading

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
print(f"Project root added to sys.path: {project_root}")

# Load environment variables from .env file relative to project root
load_dotenv(dotenv_path=project_root / '.env')

from src.agents.context import ContextProtocol
from src.agents.coordinator import CoordinatorAgent
# Import TrinoConnectionManager
from src.trino_connection.connection_manager import TrinoConnectionManager

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def test_metadata_agent():
    # --- Original Query ---
    # query = "Provide total revenue by category and actor"
    # --- Query that resulted in successful SQL generation previously ---
    query = "Compare the average rental duration for staff members Warner and Lavone, considering only rentals that were returned. Additionally, show their monthly rental performance by displaying the number of rentals they completed each month."
    project_id = "0d9e2c6f-5e1b-44ba-939f-32b584161b7b" # Ensure this project exists

    # --- Load Trino details from environment ---
    trino_details = {
        "TRINO_HOST": os.getenv("TRINO_HOST"),
        "TRINO_PORT": os.getenv("TRINO_PORT"),
        "TRINO_USERNAME": os.getenv("TRINO_USERNAME"), # Use correct name from .env
        "TRINO_PASSWORD": os.getenv("TRINO_PASSWORD"),
        "TRINO_HTTP_SCHEME": os.getenv("TRINO_HTTP_SCHEME", "http"),
        "TRINO_VERIFY_SSL": os.getenv("TRINO_VERIFY_SSL", "true").lower() == "true", # Use correct name and convert
    }
    # Basic validation (optional but recommended)
    if not all([trino_details["TRINO_HOST"], trino_details["TRINO_PORT"], trino_details["TRINO_USERNAME"]]):
         print("ERROR: Missing required Trino connection environment variables (TRINO_HOST, TRINO_PORT, TRINO_USERNAME) in .env file!")
         sys.exit(1) # Exit if required details are missing

    # --- Create Trino Connection Manager instance ---
    try:
        conn_manager = TrinoConnectionManager(
            host=trino_details["TRINO_HOST"],
            port=int(trino_details["TRINO_PORT"]), # Ensure port is int
            user=trino_details["TRINO_USERNAME"], # Pass correct user
            password=trino_details["TRINO_PASSWORD"],
            http_scheme=trino_details["TRINO_HTTP_SCHEME"],
            verify=trino_details["TRINO_VERIFY_SSL"] # Pass correct verify flag
        )
    except Exception as e:
        print(f"ERROR: Failed to create TrinoConnectionManager: {e}")
        sys.exit(1) # Exit if connection manager can't be created

    # --- Create context (WITHOUT Trino details) ---
    # Pass only required positional arguments to __init__
    context = ContextProtocol(query=query, project_id=project_id)
    # __post_init__ will create _data and default 'clarifications': {}

    # --- Initialize Coordinator with injected connection manager ---
    coordinator = CoordinatorAgent(context, connection_manager=conn_manager)

    while True:
        result = await coordinator.execute()
        print(f"\nResult: {result}")

        if result.operation == "clarification_needed":
            clarifications = {}
            print("\n--- CLARIFICATION REQUIRED ---")
            for ambiguity in result.details["ambiguities"]:
                question = ambiguity["question"]
                suggestion = ambiguity["suggestion"]
                print(f"\nQ: {question}")
                print(f"Suggestion: {suggestion}")
                # Require manual input for clarification
                answer = input("Your response (press Enter to accept suggestion): ").strip()
                clarifications[question] = answer if answer else suggestion # Use suggestion if user enters nothing
            print("-----------------------------")
            # Log and update context with clarifications
            print(f"Updating context with clarifications: {clarifications}")
            # Update the context on the existing coordinator instance
            coordinator.context = coordinator.context.update({"clarifications": clarifications})
            # Continue the loop to re-run execute with clarifications
        elif result.operation == "error":
            print(f"\n--- WORKFLOW ERROR ---")
            print(f"Error: {result.details.get('error', 'Unknown error')}")
            if 'sql_query' in result.details:
                print(f"Failed SQL Query:\n{result.details['sql_query']}")
            print("----------------------")
            break
        elif result.operation == "data_retrieval":
             # Added check for this step
             print(f"\n--- DATA RETRIEVED ---")
             print(f"Status: {result.details.get('status')}")
             print(f"Rows Retrieved: {result.details.get('row_count')}")
             print("----------------------")
             # Continue loop, Coordinator should handle transition
        elif result.operation == "transformation_complete":
             # Added check for this step
             print(f"\n--- TRANSFORMATION COMPLETE (Placeholder) ---")
             print(f"Message: {result.details.get('message')}")
             print(f"Sample Data: {result.details.get('data', [])[:3]}") # Show sample
             print("---------------------------------------------")
             # Continue loop, Coordinator should handle transition
        elif result.operation == "complete":
            print(f"\n--- WORKFLOW COMPLETE ---")
            print(f"Message: {result.details.get('message')}")
            print(f"Output File: {result.details.get('output_file')}") # Explicitly print file path
            print(f"Final Row Count: {result.details.get('row_count')}")
            print(f"Final Data Sample (from CSV): {result.details.get('final_data_sample')}")
            print("-------------------------")
            break
        else:
             # Catch unexpected states/operations
             print(f"\n--- UNEXPECTED STATE ---")
             print(f"Ended with operation: {result.operation}")
             print(f"Details: {result.details}")
             print("------------------------")
             break # Stop if state is unknown

    print("\nFinal Context Snapshot:", json.dumps(coordinator.context.snapshot(), indent=2))
        
if __name__ == "__main__":
    asyncio.run(test_metadata_agent())

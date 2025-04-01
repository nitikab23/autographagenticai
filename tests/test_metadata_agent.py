import sys
from pathlib import Path
import json
import asyncio
import logging

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
print(f"Project root added to sys.path: {project_root}")

from src.agents.context import ContextProtocol
from src.agents.coordinator import CoordinatorAgent

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def test_metadata_agent():
    query = "Count rentals for every store and film title"
    project_id = "0d9e2c6f-5e1b-44ba-939f-32b584161b7b"

    project_dir = Path("storage/metadata/projects") / project_id
    #metadata_path = project_dir / "tables_metadata.json"

    context = ContextProtocol(query=query, project_id=project_id)
    coordinator = CoordinatorAgent(context)

    while True:
        result = await coordinator.execute()
        print(f"\nResult: {result}")

        if result.operation == "clarification_needed":
            clarifications = {}
            for ambiguity in result.details["ambiguities"]:
                question = ambiguity["question"]
                suggestion = ambiguity["suggestion"]
                print(f"\nClarification needed: {question}")
                print(f"Suggestion: {suggestion}")
                answer = input("Your response (press Enter to accept suggestion): ").strip()
                clarifications[question] = answer if answer else suggestion
            # Log and update context with clarifications
            print(f"Provided clarifications: {clarifications}")
            coordinator.context = coordinator.context.update({"clarifications": clarifications})
        elif result.operation == "error":
            print(f"Error: {result.details['error']}")
            break
        elif result.operation == "sql_generation":
            print("SQL Query Generated:")
            print(result.details["sql_query"])
            break
        elif result.operation == "complete":
            print("Workflow completed")
            break

    print("\nFinal Context Snapshot:", json.dumps(coordinator.context.snapshot(), indent=2))

if __name__ == "__main__":
    asyncio.run(test_metadata_agent())
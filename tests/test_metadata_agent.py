
import sys
import os
from pathlib import Path
import json

# Add project root to Python path
project_root = str(Path(__file__).parent.parent)
sys.path.insert(0, project_root)

import asyncio
import logging
from src.agents.context import ContextProtocol
from src.agents.core import CoordinatorAgent

# Set up detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def test_metadata_agent():
    query = "List top 3 actors by total revenue for comedies in the last 6 months"
    project_id = "0d9e2c6f-5e1b-44ba-939f-32b584161b7b"

    # Verify project directory exists
    project_dir = Path("storage/metadata/projects") / project_id
    if not project_dir.exists():
        print(f"Project directory does not exist: {project_dir}")
        # Create directory structure
        project_dir.mkdir(parents=True, exist_ok=True)
        print(f"Created project directory and metadata file with initial content")

    context = ContextProtocol(query=query, project_id=project_id)
    coordinator = CoordinatorAgent(context)
    result = await coordinator.execute()

    print("\nResult:", result)
    print("\nContext Snapshot:", context.snapshot())

if __name__ == "__main__":
    asyncio.run(test_metadata_agent())

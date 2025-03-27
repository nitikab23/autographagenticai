import os
import sys
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.service_layer import ApplicationService

def cleanup_projects():
    service = ApplicationService()
    
    # List all projects
    projects = service.list_projects()
    
    if not projects:
        print("No projects found.")
        return
    
    print("\nExisting projects:")
    for project in projects:
        print(f"ID: {project['id']}")
        print(f"Name: {project['name']}")
        print(f"Description: {project['description']}")
        print(f"Data sources count: {project['data_sources_count']}")
        print("-" * 50)
    
    # Ask for confirmation
    confirm = input("\nDo you want to delete all these projects? (yes/no): ").lower()
    
    if confirm == 'yes':
        for project in projects:
            try:
                result = service.delete_project(project['id'])
                print(f"Deleted project {project['id']}: {result['message']}")
            except Exception as e:
                print(f"Failed to delete project {project['id']}: {str(e)}")
    else:
        print("Operation cancelled.")

if __name__ == "__main__":
    load_dotenv()
    cleanup_projects()
import os
import logging
from agent_engine import BigQuerySharingAgent

# Set up logging
logging.basicConfig(level=logging.INFO)

def test_agent_search():
    print("Testing Agent Search...")
    
    # Mock Project/Location
    project_id = os.environ.get("PROJECT_ID", "my-project-id")
    location = os.environ.get("LOCATION", "us-central1")
    
    agent = BigQuerySharingAgent(project_id=project_id, location=location)
    
    # Test Input
    query = "sales data"
    state_input = {"query": query, "messages": []}
    
    print(f"Invoking agent with query: '{query}'")
    response = agent.invoke(state_input)
    
    # Check Output
    listings = response.get("listings", [])
    print(f"Found {len(listings)} listings.")
    
    for listing in listings:
        print(f"- {listing.get('display_name')} (Quality: {listing.get('data_quality_score')})")

def test_agent_subscription():
    print("\nTesting Agent Subscription...")
    # This requires a valid listing ID, which we might not have without real search results.
    # We'll skip for now or mock it if we had a known ID.
    pass

if __name__ == "__main__":
    test_agent_search()

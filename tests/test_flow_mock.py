import unittest
from unittest.mock import MagicMock, patch
import json
from langchain_core.messages import AIMessage

# Import the code to test
# Assuming agent_engine.py is in the parent directory or pythonpath
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from agent_engine import BigQuerySharingAgent

class TestBigQuerySharingAgent(unittest.TestCase):
    
    @patch('agent_engine.ChatVertexAI')
    @patch('agent_engine.bq_tools')
    @patch('agent_engine.dataplex_tools')
    def test_search_flow(self, mock_dataplex, mock_bq, mock_llm_class):
        print("\n--- Testing Search Flow (Mocked) ---")
        
        # 1. Setup Mocks
        # Mock LLM (Vertex AI)
        mock_llm_instance = MagicMock()
        mock_llm_class.return_value = mock_llm_instance
        # The agent uses the LLM in generate_response_node, but currently that node just dumps JSON.
        # However, the init calls ChatVertexAI.
        
        # Mock BigQuery Tool
        mock_bq.search_listings.return_value = [
            {
                "name": "projects/p/locations/l/exchanges/e/listings/listing1",
                "display_name": "Global Sales Data",
                "description": "Sales data for 2024",
                "listing_id": "listing1"
            },
            {
                "name": "projects/p/locations/l/exchanges/e/listings/listing2",
                "display_name": "Marketing Clickstream",
                "description": "Click events",
                "listing_id": "listing2"
            }
        ]
        
        # Mock Dataplex Tool
        mock_dataplex.get_data_quality_score.return_value = 0.98
        mock_dataplex.get_data_contract_info.return_value = {"status": "verified"}

        # 2. Initialize Agent
        agent = BigQuerySharingAgent(project_id="test-project", location="us-central1")
        
        # 3. Simulate Input
        query = "Find sales data"
        print(f"User Query: {query}")
        result = agent.invoke({"query": query, "messages": []})
        
        # 4. Verify Search Results
        listings = result.get("listings")
        print(f"Agent Configured Listings: {len(listings)}")
        self.assertEqual(len(listings), 2)
        
        first_item = listings[0]
        self.assertEqual(first_item["display_name"], "Global Sales Data")
        
        # 5. Verify Enrichment (Dataplex)
        print("Verifying Dataplex Enrichment...")
        self.assertEqual(first_item["data_quality_score"], 0.98)
        self.assertEqual(first_item["data_contract"]["status"], "verified")
        print("✅ Search & Enrichment Flow Verified")

    @patch('agent_engine.ChatVertexAI')
    @patch('agent_engine.bq_tools')
    def test_subscription_flow(self, mock_bq, mock_llm_class):
        print("\n--- Testing Subscription Flow (Mocked) ---")
        
        mock_bq.subscribe_listing.return_value = "Success: Subscribed to listing1"
        
        agent = BigQuerySharingAgent(project_id="test-project")
        
        # Simulate button click state
        state_input = {
            "selected_listing_id": "projects/p/locations/l/exchanges/e/listings/listing1",
            "query": "subscribe",
            "messages": []
        }
        
        print(f"User Action: Subscribe to {state_input['selected_listing_id']}")
        result = agent.invoke(state_input)
        
        sub_result = result.get("subscription_result")
        print(f"Subscription Result: {sub_result}")
        
        self.assertEqual(sub_result, "Success: Subscribed to listing1")
        print("✅ Subscription Flow Verified")

if __name__ == '__main__':
    unittest.main()

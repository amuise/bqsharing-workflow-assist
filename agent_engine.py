from typing import TypedDict, Annotated, List, Optional
import operator
from langgraph.graph import StateGraph, END
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, SystemMessage
from langchain_google_vertexai import ChatVertexAI
from tools import bq_tools, dataplex_tools
import json

# Define the state of the agent
class AgentState(TypedDict):
    messages: Annotated[List[BaseMessage], operator.add]
    query: str
    listings: Optional[List[dict]]
    selected_listing_id: Optional[str]
    subscription_result: Optional[str]

class BigQuerySharingAgent:
    def __init__(self, project_id: str, location: str = "us-central1"):
        self.project_id = project_id
        self.location = location
        self.llm = ChatVertexAI(model_name="gemini-1.5-pro", temperature=0)
        self.graph = self._build_graph()

    def _build_graph(self):
        workflow = StateGraph(AgentState)

        # Define nodes
        workflow.add_node("search_listings", self.search_listings_node)
        workflow.add_node("enrich_listings", self.enrich_listings_node)
        workflow.add_node("rank_listings", self.rank_listings_node)
        workflow.add_node("generate_response", self.generate_response_node)
        workflow.add_node("subscribe_listing", self.subscribe_listing_node)

        # Define edges
        # We need a conditional edge to decide if we are searching or subscribing
        workflow.set_entry_point("determine_intent")
        
        workflow.add_conditional_edges(
            "determine_intent",
            self.route_intent,
            {
                "search": "search_listings",
                "subscribe": "subscribe_listing"
            }
        )

        workflow.add_edge("search_listings", "enrich_listings")
        workflow.add_edge("enrich_listings", "rank_listings")
        workflow.add_edge("rank_listings", "generate_response")
        workflow.add_edge("generate_response", END)
        workflow.add_edge("subscribe_listing", END) # After subscribing, we are done
        
        # Add the determine_intent node which is just a pass-through
        workflow.add_node("determine_intent", lambda state: state)

        return workflow.compile()

    def route_intent(self, state: AgentState):
        """Determines if the user wants to search or subscribe based on state."""
        if state.get("selected_listing_id"):
            return "subscribe"
        return "search"

    def search_listings_node(self, state: AgentState):
        query = state.get("query", "")
        # Extract query from messages if not explicitly in state
        if not query and state["messages"]:
            query = state["messages"][-1].content
            
        print(f"Searching for: {query}")
        results = bq_tools.search_listings(query, self.project_id, self.location)
        return {"listings": results, "query": query}

    def enrich_listings_node(self, state: AgentState):
        listings = state.get("listings", [])
        enriched_listings = []
        
        # For top 3 listings, fetch metadata
        # (Optimizing to avoid too many API calls)
        for listing in listings[:3]:
            # Mocking entry ID mapping
            # In reality, listing.name might need parsing to find the corresponding Dataplex Entry
            entry_id = listing.get("name") 
            quality = dataplex_tools.get_data_quality_score(entry_id)
            contract = dataplex_tools.get_data_contract_info(entry_id)
            
            enrichment = {
                **listing,
                "data_quality_score": quality,
                "data_contract": contract
            }
            enriched_listings.append(enrichment)
            
        return {"listings": enriched_listings}

    def rank_listings_node(self, state: AgentState):
        # We could use the LLM to re-rank here based on the query and metadata context
        # For now, we will just return the enriched listings as is, or sorted by quality
        listings = state.get("listings", [])
        listings.sort(key=lambda x: x.get("data_quality_score", 0), reverse=True)
        return {"listings": listings}

    def generate_response_node(self, state: AgentState):
        listings = state.get("listings", [])
        
        if not listings:
            return {"messages": [AIMessage(content="I couldn't find any data listings matching your request.")]}
            
        # We don't construct the full Block Kit JSON here because the Agent Engine 
        # outputs text/JSON that the Slack App parses.
        # We will iterate and return a JSON string or structured list.
        
        return {"messages": [AIMessage(content=json.dumps(listings))]}

    def subscribe_listing_node(self, state: AgentState):
        listing_id = state.get("selected_listing_id")
        # Ensure we have a destination dataset. 
        # For this PoC, we might auto-generate one or ask the user.
        # We'll assume a default or require it in the input.
        destination = f"subscription_{listing_id.split('/')[-1]}"
        
        result = bq_tools.subscribe_listing(listing_id, destination, self.project_id, self.location)
        return {"subscription_result": result}

    def invoke(self, input_state: dict):
        return self.graph.invoke(input_state)

# For Vertex AI Agent Engine, we might need to expose a specific function or class method
# depending on the deployment pattern. 
# Usually `agent = reasoning_engines.LangchainAgent(...)`

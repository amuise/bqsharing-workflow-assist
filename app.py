import os
import logging
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from agent_engine import BigQuerySharingAgent
import json

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Bolt App
# In production, use os.environ for tokens
app = App(token=os.environ.get("SLACK_BOT_TOKEN"))

# Initialize the Agent
# In production, we would call the Reasoning Engine API here.
# For this implementation, we run the agent logic locally within the same process.
PROJECT_ID = os.environ.get("PROJECT_ID", "my-project-id")
LOCATION = os.environ.get("LOCATION", "us-central1")
agent = BigQuerySharingAgent(project_id=PROJECT_ID, location=LOCATION)

@app.command("/find-data")
def handle_find_data(ack, body, logger):
    ack()
    user_query = body.get("text")
    user_id = body.get("user_id")
    
    # 1. Invoke Agent
    logger.info(f"User {user_id} requested: {user_query}")
    
    # Run the agent graph
    state_input = {"query": user_query, "messages": []}
    response = agent.invoke(state_input)
    
    # 2. Process Response
    # The agent returns the final state. We expect `listings` in it.
    listings = response.get("listings", [])
    
    if not listings:
        app.client.chat_postMessage(
            channel=body["channel_id"],
            text=f"Sorry, I couldn't find any data listings for '{user_query}'."
        )
        return

    # 3. Build Block Kit UI
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"Data Search Results for: {user_query}",
                "emoji": True
            }
        },
        {"type": "divider"}
    ]
    
    for listing in listings[:5]: # proper limit for slack block limits
        listing_id = listing.get("listing_id")
        display_name = listing.get("display_name")
        description = listing.get("description", "No description")
        quality_score = listing.get("data_quality_score", "N/A")
        
        # Section with details
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*<{listing.get('url', '#')}|{display_name}>*\n{description}\n*Quality Score:* {quality_score}"
            }
        })
        
        # Action Buttons
        blocks.append({
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "View in Console",
                        "emoji": True
                    },
                    "url": f"https://console.cloud.google.com/bigquery/analytics-hub/listings/{listing_id}?project={PROJECT_ID}",
                    "action_id": "view_console"
                },
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Subscribe",
                        "emoji": True
                    },
                    "style": "primary",
                    "value": listing.get("name"), # Pass the full resource name
                    "action_id": "subscribe_listing"
                }
            ]
        })
        blocks.append({"type": "divider"})

    # Send blocks
    app.client.chat_postMessage(
        channel=body["channel_id"],
        blocks=blocks,
        text=f"Found {len(listings)} listings for '{user_query}'" # Fallback text
    )

@app.action("subscribe_listing")
def handle_subscription(ack, body, logger):
    ack()
    user_id = body["user"]["id"]
    listing_name = body["actions"][0]["value"]
    
    logger.info(f"User {user_id} subscribing to: {listing_name}")
    
    # Invoke Agent Subscription Logic
    # We pass the selected listing ID to route to the subscribe node
    state_input = {
        "selected_listing_id": listing_name,
        "query": "subscribe", # Dummy query
        "messages": []
    }
    
    response = agent.invoke(state_input)
    result_message = response.get("subscription_result", "Subscription failed.")
    
    # Notify user
    app.client.chat_postMessage(
        channel=body["channel"]["id"],
        text=f"<@{user_id}> {result_message}"
    )

if __name__ == "__main__":
    # Start Socket Mode handler
    handler = SocketModeHandler(app, os.environ.get("SLACK_APP_TOKEN"))
    handler.start()

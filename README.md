# BigQuery Sharing Workflow Assistant

An AI-powered Slack assistant that helps teams discover, evaluate, and subscribe to data listings in **BigQuery Analytics Hub**.

Powered by **Google Cloud Vertex AI Agent Engine** and **LangGraph**, this agent intelligently searches listings, enriches them with **Dataplex** governance metadata (Data Quality, Contracts), and facilitates one-click subscriptions directly from Slack.

## Features

- 🔍 **Natural Language Search**: Ask "Find sales data for 2024" and get relevant results from BigQuery Analytics Hub.
- 🛡️ **Governance-Aware Ranking**: Surfaces Data Quality scores and Data Contract status (via Dataplex) to help you choose the *best* data.
- 💬 **Slack Integration**: Native Block Kit UI for browsing results and subscribing without context switching.
- 🔗 **Deep Linking**: Direct links to the Google Cloud Console for deep dives into schema and lineage.
- 🤖 **Agentic Workflow**: Uses LangGraph to manage the conversation state and tool execution.

## Architecture

1.  **Slack App (Frontend)**: A lightweight Python app (`slack_bolt`) listening for slash commands and events.
2.  **Agent Engine (Backend)**: Defines the reasoning logic using **LangGraph** around a Vertex AI model.
3.  **Tools**:
    - `bq_tools.py`: Interacts with the BigQuery Analytics Hub API for search an subscription.
    - `dataplex_tools.py`: Fetches metadata from Dataplex Universal Catalog.

## Prerequisites

- **Google Cloud Project** with billing enabled.
- **APIs Enabled**:
    - `bigquery.googleapis.com`
    - `analyticshub.googleapis.com`
    - `dataplex.googleapis.com`
    - `aiplatform.googleapis.com`
- **IAM Permissions**:
    - `BigQuery Data Exchange Listing User`
    - `Dataplex Metadata Reader`
    - `Vertex AI User`

## Installation

1.  **Clone the repository**:
    ```bash
    git clone <your-repo-url>
    cd bqsharing-workflow-assist
    ```

2.  **Set up a Virtual Environment**:
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```

3.  **Install Dependencies**:
    ```bash
    pip install google-cloud-bigquery-data-exchange google-cloud-dataplex google-cloud-aiplatform langgraph langchain-google-vertexai slack_bolt
    ```

## Configuration

Set the following environment variables:

```bash
export PROJECT_ID="your-gcp-project-id"
export LOCATION="us-central1"
export SLACK_BOT_TOKEN="xoxb-..."
export SLACK_APP_TOKEN="xapp-..."
```

## Usage

### Running Locally (Demo Mode)
The app is configured to run the agent logic locally for development.

1.  Start the Slack app:
    ```bash
    python app.py
    ```
2.  In Slack, type:
    `/find-data marketing data`

### Running Tests
Verify the agent logic without Slack or full GCP credentials using the mocked test script:

```bash
python tests/test_flow_mock.py
```

## Deployment

To deploy the agent to **Vertex AI Agent Engine** (Reasoning Engine), refer to the official Google Cloud documentation on determining the `reasoning_engines` resource.

Future updates will include a `deploy.py` script to automate this process.

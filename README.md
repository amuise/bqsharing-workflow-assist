# BigQuery Sharing Workflow Assistant

An AI-powered Slack assistant that helps teams discover, evaluate, and subscribe to data listings in **BigQuery Analytics Hub**.

Powered by **Google Cloud Vertex AI Agent Engine** and **LangGraph**, this agent intelligently searches listings, cross-references them with the **Dataplex Data Product catalog**, enriches them with governance metadata (Data Quality, Contracts), and facilitates one-click subscriptions directly from Slack.

## Features

- 🔍 **Natural Language Search**: Ask "Find sales data for 2024" and get relevant results from BigQuery Analytics Hub.
- 🗂️ **Data Product Enrichment**: Automatically cross-references BigQuery listings against the Dataplex Universal Catalog. When a listing is also registered as a Data Product, additional metadata is merged in — including owner team, domain, data classification, SLA tier, and status — with no duplicated fields.
- 🛡️ **Governance-Aware Ranking**: Surfaces Data Quality scores and Data Contract status (via Dataplex) to help you choose the *best* data.
- 💬 **Slack Integration**: Native Block Kit UI for browsing results and subscribing without context switching.
- 🔗 **Deep Linking**: Direct links to the Google Cloud Console for deep dives into schema and lineage.
- 🤖 **Agentic Workflow**: Uses LangGraph to manage the conversation state and tool execution.

## Architecture

1.  **Slack App (Frontend)**: A lightweight Python app (`slack_bolt`) listening for slash commands and events.
2.  **Agent Engine (Backend)**: Defines the reasoning logic using **LangGraph** around a Vertex AI model.
3.  **Tools**:
    - `bq_tools.py`: Interacts with the BigQuery Analytics Hub API for search and subscription.
    - `dataplex_tools.py`: Fetches Data Quality scores and Data Contract info from Dataplex.
    - `data_product_tools.py`: Searches the Dataplex Universal Catalog for Data Product entries and merges them with matching BigQuery listings.

### Agent Pipeline

```
search_listings → enrich_with_data_products → enrich_listings → rank_listings → generate_response
```

| Node | What it does |
|---|---|
| `search_listings` | Queries BigQuery Analytics Hub across all exchanges |
| `enrich_with_data_products` | Matches each listing to a Dataplex Data Product; merges unique fields and surfaces any conflicting metadata |
| `enrich_listings` | Adds Data Quality scores and Data Contract status via Dataplex |
| `rank_listings` | Sorts by data quality score |
| `generate_response` | Serialises results for the Slack app |

### Data Product Merging

A listing is matched to a data product by a **strict equality check on the normalized display name** (lower-cased, with surrounding and repeated internal whitespace collapsed). This assumes products are co-published to Analytics Hub and the Data Product API with identical or near-identical names. Substring/fuzzy matching is intentionally avoided so an unrelated product cannot hijack a listing's surfaced governance metadata.

When a BigQuery listing is matched to a Dataplex Data Product, the two records are merged:

- **Shared fields** (`display_name`, `description`): the BigQuery value is kept as primary; if the Data Product carries a different value it is surfaced under `conflicting_fields` for transparency.
- **Data Product-only fields** are collected under `data_product_unique_fields` and include: `owner_team`, `domain`, `data_classification`, `contact_email`, `status`, `sla_tier`, `update_frequency`, `documentation_url`, `linked_resources`.
- **BigQuery-only fields** (`data_exchange`, `listing_id`, `exchange_id`, subscription info) are always preserved.

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
    - `Dataplex Catalog Editor` (to read Data Product entries)
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

    > `google-cloud-dataplex` covers both Dataplex governance metadata and the Universal Catalog (Data Product) API — no additional package is required.

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
Verify the agent logic without Slack or full GCP credentials using the mocked test scripts:

```bash
# Core search and subscription flows
python tests/test_flow_mock.py

# Data Product API integration and merge logic
python tests/test_data_product_tools.py
```

## Deployment

To deploy the agent to **Vertex AI Agent Engine** (Reasoning Engine), refer to the official Google Cloud documentation on determining the `reasoning_engines` resource.

Future updates will include a `deploy.py` script to automate this process.

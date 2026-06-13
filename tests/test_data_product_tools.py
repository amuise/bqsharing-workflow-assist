"""
Tests for tools/data_product_tools.py and the agent's data-product enrichment node.

Unit tests cover:
  - search_data_products (happy path, filtering, API error)
  - get_data_product (happy path, API error)
  - find_matching_product (exact, partial, case-insensitive, no match)
  - merge_listing_with_data_product (unique fields, conflicts, clean merge)

Integration tests (all GCP/LLM calls mocked) cover:
  - Full agent flow with data-product enrichment when a match is found
  - Full agent flow when no data product matches
"""

import sys
import os
import unittest
from unittest.mock import MagicMock, patch, PropertyMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from tools import data_product_tools
from tools.data_product_tools import (
    find_matching_product,
    merge_listing_with_data_product,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_entry(
    name="projects/p/locations/l/entryGroups/eg/entries/prod1",
    display_name="Global Sales Data",
    description="Curated sales dataset",
    entry_type="projects/p/locations/l/entryTypes/data-product",
    aspects=None,
):
    """Build a minimal mock Dataplex Entry proto-like object."""
    entry = MagicMock()
    entry.name = name
    entry.display_name = display_name
    entry.description = description
    entry.entry_type = entry_type
    entry.create_time = None
    entry.update_time = None
    entry.aspects = aspects or {}
    return entry


def _make_search_result(entry):
    result = MagicMock()
    result.entry = entry
    return result


def _make_bq_listing(
    display_name="Global Sales Data",
    description="Sales data for 2024",
    listing_id="listing1",
):
    return {
        "name": f"projects/p/locations/US/dataExchanges/ex/listings/{listing_id}",
        "display_name": display_name,
        "description": description,
        "data_exchange": "My Exchange",
        "listing_id": listing_id,
        "project_id": "p",
        "location": "US",
        "exchange_id": "ex",
    }


# ---------------------------------------------------------------------------
# search_data_products
# ---------------------------------------------------------------------------

class TestSearchDataProducts(unittest.TestCase):

    @patch("tools.data_product_tools.dataplex_v1.CatalogServiceClient")
    def test_returns_matching_data_products(self, mock_client_class):
        entry = _make_entry()
        mock_client = mock_client_class.return_value
        mock_client.search_entries.return_value = [_make_search_result(entry)]

        results = data_product_tools.search_data_products("sales", "my-project")

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["display_name"], "Global Sales Data")
        self.assertEqual(results[0]["name"], entry.name)

    @patch("tools.data_product_tools.dataplex_v1.CatalogServiceClient")
    def test_filters_out_non_data_product_entries(self, mock_client_class):
        non_product_entry = _make_entry(entry_type="projects/p/locations/l/entryTypes/table")
        product_entry = _make_entry(name="projects/p/locations/l/entryGroups/eg/entries/p2",
                                    display_name="Marketing Data",
                                    entry_type="projects/p/locations/l/entryTypes/data-product")

        mock_client = mock_client_class.return_value
        mock_client.search_entries.return_value = [
            _make_search_result(non_product_entry),
            _make_search_result(product_entry),
        ]

        results = data_product_tools.search_data_products("data", "my-project")

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["display_name"], "Marketing Data")

    @patch("tools.data_product_tools.dataplex_v1.CatalogServiceClient")
    def test_returns_empty_list_on_api_error(self, mock_client_class):
        from google.api_core import exceptions as gcp_exceptions
        mock_client = mock_client_class.return_value
        mock_client.search_entries.side_effect = gcp_exceptions.GoogleAPICallError("boom")

        results = data_product_tools.search_data_products("sales", "my-project")

        self.assertEqual(results, [])

    @patch("tools.data_product_tools.dataplex_v1.CatalogServiceClient")
    def test_returns_empty_list_when_no_results(self, mock_client_class):
        mock_client = mock_client_class.return_value
        mock_client.search_entries.return_value = []

        results = data_product_tools.search_data_products("nonexistent", "my-project")

        self.assertEqual(results, [])


# ---------------------------------------------------------------------------
# get_data_product
# ---------------------------------------------------------------------------

class TestGetDataProduct(unittest.TestCase):

    @patch("tools.data_product_tools.dataplex_v1.CatalogServiceClient")
    def test_returns_normalised_product(self, mock_client_class):
        entry = _make_entry()
        mock_client = mock_client_class.return_value
        mock_client.get_entry.return_value = entry

        result = data_product_tools.get_data_product(entry.name)

        self.assertEqual(result["display_name"], "Global Sales Data")
        self.assertEqual(result["entry_type"], entry.entry_type)

    @patch("tools.data_product_tools.dataplex_v1.CatalogServiceClient")
    def test_returns_empty_dict_on_api_error(self, mock_client_class):
        from google.api_core import exceptions as gcp_exceptions
        mock_client = mock_client_class.return_value
        mock_client.get_entry.side_effect = gcp_exceptions.GoogleAPICallError("not found")

        result = data_product_tools.get_data_product("projects/p/locations/l/eg/entries/missing")

        self.assertEqual(result, {})


# ---------------------------------------------------------------------------
# find_matching_product
# ---------------------------------------------------------------------------

class TestFindMatchingProduct(unittest.TestCase):

    def setUp(self):
        self.products = [
            {"name": "dp1", "display_name": "Global Sales Data"},
            {"name": "dp2", "display_name": "Marketing Clickstream"},
            {"name": "dp3", "display_name": "Finance Reports"},
        ]

    def test_exact_name_match(self):
        listing = _make_bq_listing(display_name="Global Sales Data")
        result = find_matching_product(listing, self.products)
        self.assertIsNotNone(result)
        self.assertEqual(result["name"], "dp1")

    def test_case_insensitive_match(self):
        listing = _make_bq_listing(display_name="global sales data")
        result = find_matching_product(listing, self.products)
        self.assertIsNotNone(result)
        self.assertEqual(result["name"], "dp1")

    def test_whitespace_normalized_match(self):
        # Leading/trailing and repeated internal whitespace must not defeat the match.
        listing = _make_bq_listing(display_name="  Global   Sales  Data ")
        result = find_matching_product(listing, self.products)
        self.assertIsNotNone(result)
        self.assertEqual(result["name"], "dp1")

    def test_listing_substring_of_product_does_not_match(self):
        # "Sales Data" is a substring of "Global Sales Data" but is NOT an exact
        # match — strict matching must reject it to avoid metadata hijacking.
        listing = _make_bq_listing(display_name="Sales Data")
        result = find_matching_product(listing, self.products)
        self.assertIsNone(result)

    def test_product_substring_of_listing_does_not_match(self):
        # "Finance Reports" is a substring of the listing name but not equal.
        listing = _make_bq_listing(display_name="Finance Reports Q4 2024")
        result = find_matching_product(listing, self.products)
        self.assertIsNone(result)

    def test_no_match_returns_none(self):
        listing = _make_bq_listing(display_name="Unrelated Dataset XYZ")
        result = find_matching_product(listing, self.products)
        self.assertIsNone(result)

    def test_empty_products_returns_none(self):
        listing = _make_bq_listing()
        result = find_matching_product(listing, [])
        self.assertIsNone(result)

    def test_listing_with_no_display_name_returns_none(self):
        listing = {"name": "projects/p/l/e/listings/l1", "display_name": ""}
        result = find_matching_product(listing, self.products)
        self.assertIsNone(result)


# ---------------------------------------------------------------------------
# merge_listing_with_data_product
# ---------------------------------------------------------------------------

class TestMergeListingWithDataProduct(unittest.TestCase):

    def _base_product(self, **kwargs):
        base = {
            "name": "projects/p/locations/l/entryGroups/eg/entries/dp1",
            "display_name": "Global Sales Data",
            "description": "Curated sales dataset",
            "owner_team": "data-team-alpha",
            "domain": "sales",
            "data_classification": "internal",
            "contact_email": "data@example.com",
            "status": "production",
            "sla_tier": "gold",
        }
        base.update(kwargs)
        return base

    def test_unique_data_product_fields_are_collected(self):
        listing = _make_bq_listing()
        product = self._base_product()

        merged = merge_listing_with_data_product(listing, product)

        unique = merged["data_product_unique_fields"]
        self.assertIn("owner_team", unique)
        self.assertEqual(unique["owner_team"], "data-team-alpha")
        self.assertIn("domain", unique)
        self.assertIn("data_classification", unique)
        self.assertIn("status", unique)
        self.assertIn("sla_tier", unique)

    def test_bq_listing_fields_are_preserved(self):
        listing = _make_bq_listing()
        product = self._base_product()

        merged = merge_listing_with_data_product(listing, product)

        self.assertEqual(merged["listing_id"], "listing1")
        self.assertEqual(merged["data_exchange"], "My Exchange")
        self.assertEqual(merged["exchange_id"], "ex")

    def test_data_product_name_stored_separately(self):
        listing = _make_bq_listing()
        product = self._base_product()

        merged = merge_listing_with_data_product(listing, product)

        self.assertEqual(
            merged["data_product_name"],
            "projects/p/locations/l/entryGroups/eg/entries/dp1",
        )
        # Original BQ listing name is unchanged
        self.assertIn("projects/p/locations/US", merged["name"])

    def test_conflicting_description_surfaced(self):
        listing = _make_bq_listing(description="Sales data for 2024")
        product = self._base_product(description="Curated global sales dataset — 2024")

        merged = merge_listing_with_data_product(listing, product)

        self.assertIn("conflicting_fields", merged)
        conflict = merged["conflicting_fields"]["description"]
        self.assertEqual(conflict["bq_value"], "Sales data for 2024")
        self.assertEqual(conflict["dp_value"], "Curated global sales dataset — 2024")
        # BQ value remains primary
        self.assertEqual(merged["description"], "Sales data for 2024")

    def test_no_conflict_when_descriptions_match(self):
        listing = _make_bq_listing(description="Sales data for 2024")
        product = self._base_product(description="Sales data for 2024")

        merged = merge_listing_with_data_product(listing, product)

        self.assertNotIn("conflicting_fields", merged)

    def test_empty_data_product_produces_empty_unique_fields(self):
        listing = _make_bq_listing()
        product = {"name": "projects/p/l/eg/entries/dp1", "display_name": "Global Sales Data"}

        merged = merge_listing_with_data_product(listing, product)

        self.assertEqual(merged["data_product_unique_fields"], {})
        self.assertNotIn("conflicting_fields", merged)

    def test_none_dp_value_does_not_create_conflict(self):
        listing = _make_bq_listing(description="Some desc")
        product = self._base_product(description=None)  # None → no conflict

        merged = merge_listing_with_data_product(listing, product)

        self.assertNotIn("conflicting_fields", merged)


# ---------------------------------------------------------------------------
# Integration: agent flow with data-product enrichment
# ---------------------------------------------------------------------------

class TestAgentDataProductEnrichmentFlow(unittest.TestCase):

    def _build_agent(self, mock_llm_class):
        from agent_engine import BigQuerySharingAgent
        mock_llm_class.return_value = MagicMock()
        return BigQuerySharingAgent(project_id="test-project", location="us-central1")

    @patch("agent_engine.ChatVertexAI")
    @patch("agent_engine.data_product_tools")
    @patch("agent_engine.dataplex_tools")
    @patch("agent_engine.bq_tools")
    def test_matched_listing_is_merged_with_data_product(
        self, mock_bq, mock_dataplex, mock_dp_tools, mock_llm
    ):
        """When a BQ listing has a matching data product the final listing should
        contain data_product_unique_fields and data_product_name."""
        mock_bq.search_listings.return_value = [
            {
                "name": "projects/p/locations/US/dataExchanges/ex/listings/listing1",
                "display_name": "Global Sales Data",
                "description": "Sales data for 2024",
                "listing_id": "listing1",
            }
        ]
        mock_dataplex.get_data_quality_score.return_value = 0.95
        mock_dataplex.get_data_contract_info.return_value = {"status": "active"}

        matched_product = {
            "name": "projects/p/locations/l/entryGroups/eg/entries/dp1",
            "display_name": "Global Sales Data",
            "description": "Curated sales dataset",
            "owner_team": "data-team-alpha",
            "domain": "sales",
            "status": "production",
        }
        mock_dp_tools.search_data_products.return_value = [matched_product]
        mock_dp_tools.find_matching_product.return_value = matched_product
        mock_dp_tools.merge_listing_with_data_product.side_effect = (
            merge_listing_with_data_product
        )

        agent = self._build_agent(mock_llm)
        result = agent.invoke({"query": "Find sales data", "messages": []})

        listings = result.get("listings", [])
        self.assertEqual(len(listings), 1)

        listing = listings[0]
        self.assertIn("data_product_name", listing)
        self.assertIn("data_product_unique_fields", listing)
        unique = listing["data_product_unique_fields"]
        self.assertEqual(unique.get("owner_team"), "data-team-alpha")
        self.assertEqual(unique.get("domain"), "sales")
        self.assertEqual(unique.get("status"), "production")

    @patch("agent_engine.ChatVertexAI")
    @patch("agent_engine.data_product_tools")
    @patch("agent_engine.dataplex_tools")
    @patch("agent_engine.bq_tools")
    def test_unmatched_listing_passes_through_unchanged(
        self, mock_bq, mock_dataplex, mock_dp_tools, mock_llm
    ):
        """When no data product matches, the listing is returned without DP fields."""
        mock_bq.search_listings.return_value = [
            {
                "name": "projects/p/locations/US/dataExchanges/ex/listings/listing2",
                "display_name": "Obscure Internal Dataset",
                "description": "No matching product",
                "listing_id": "listing2",
            }
        ]
        mock_dataplex.get_data_quality_score.return_value = 0.7
        mock_dataplex.get_data_contract_info.return_value = {"status": "pending"}

        mock_dp_tools.search_data_products.return_value = []
        mock_dp_tools.find_matching_product.return_value = None

        agent = self._build_agent(mock_llm)
        result = agent.invoke({"query": "obscure dataset", "messages": []})

        listings = result.get("listings", [])
        self.assertEqual(len(listings), 1)
        self.assertNotIn("data_product_name", listings[0])
        self.assertNotIn("data_product_unique_fields", listings[0])

    @patch("agent_engine.ChatVertexAI")
    @patch("agent_engine.data_product_tools")
    @patch("agent_engine.dataplex_tools")
    @patch("agent_engine.bq_tools")
    def test_data_products_state_is_populated(
        self, mock_bq, mock_dataplex, mock_dp_tools, mock_llm
    ):
        """The agent state should expose the raw data_products list after the node runs."""
        mock_bq.search_listings.return_value = [
            {"name": "n", "display_name": "Global Sales Data", "description": "", "listing_id": "l1"}
        ]
        mock_dataplex.get_data_quality_score.return_value = 0.9
        mock_dataplex.get_data_contract_info.return_value = {}

        products = [
            {"name": "dp1", "display_name": "Global Sales Data", "owner_team": "team-x"}
        ]
        mock_dp_tools.search_data_products.return_value = products
        mock_dp_tools.find_matching_product.return_value = None  # no merge needed here

        agent = self._build_agent(mock_llm)
        result = agent.invoke({"query": "sales", "messages": []})

        self.assertEqual(result.get("data_products"), products)

    @patch("agent_engine.ChatVertexAI")
    @patch("agent_engine.data_product_tools")
    @patch("agent_engine.dataplex_tools")
    @patch("agent_engine.bq_tools")
    def test_subscription_flow_skips_data_product_enrichment(
        self, mock_bq, mock_dataplex, mock_dp_tools, mock_llm
    ):
        """Subscription flow routes directly to subscribe_listing — data product
        enrichment node must not be called."""
        mock_bq.subscribe_listing.return_value = "Success: Subscribed to listing1"

        agent = self._build_agent(mock_llm)
        result = agent.invoke({
            "selected_listing_id": "projects/p/locations/l/exchanges/e/listings/listing1",
            "query": "subscribe",
            "messages": [],
        })

        mock_dp_tools.search_data_products.assert_not_called()
        self.assertEqual(result.get("subscription_result"), "Success: Subscribed to listing1")


if __name__ == "__main__":
    unittest.main()

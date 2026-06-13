from google.cloud import dataplex_v1
from google.api_core import exceptions
import logging

logger = logging.getLogger(__name__)

# Fields that may appear in both a BQ listing and a data product entry.
# Conflicts in these fields are surfaced rather than silently overwritten.
_SHARED_FIELDS = {"display_name", "description"}


def search_data_products(
    query: str, project_id: str, location: str = "us-central1"
) -> list[dict]:
    """
    Search the Dataplex Universal Catalog for data product entries matching query.

    Args:
        query: Free-text search query.
        project_id: Google Cloud project ID.
        location: Catalog location (use "global" if products are registered globally).

    Returns:
        List of normalised data product dicts.
    """
    client = dataplex_v1.CatalogServiceClient()
    parent = f"projects/{project_id}/locations/{location}"

    try:
        request = dataplex_v1.SearchEntriesRequest(
            name=parent,
            query=query,
            order_by="relevance",
        )
        page_result = client.search_entries(request=request)

        results = []
        for search_result in page_result:
            entry = search_result.entry
            if _is_data_product(entry):
                results.append(_normalize_entry(entry))
        return results

    except exceptions.GoogleAPICallError as e:
        logger.error(f"Error searching data products: {e}")
        return []


def get_data_product(product_name: str) -> dict:
    """
    Retrieve a single data product entry by its full resource name.

    Args:
        product_name: Full resource name, e.g.
            projects/{p}/locations/{l}/entryGroups/{eg}/entries/{e}

    Returns:
        Normalised data product dict, or empty dict on error.
    """
    client = dataplex_v1.CatalogServiceClient()

    try:
        request = dataplex_v1.GetEntryRequest(
            name=product_name,
            view=dataplex_v1.EntryView.FULL,
        )
        entry = client.get_entry(request=request)
        return _normalize_entry(entry)

    except exceptions.GoogleAPICallError as e:
        logger.error(f"Error retrieving data product '{product_name}': {e}")
        return {}


def find_matching_product(
    bq_listing: dict, data_products: list[dict]
) -> dict | None:
    """
    Find the best-matching data product for a BQ Analytics Hub listing.

    Matching is done by display_name (case-insensitive substring).  Returns the
    first match, or None if no candidate is found.
    """
    listing_name = bq_listing.get("display_name", "").lower().strip()
    if not listing_name:
        return None

    for product in data_products:
        product_name = product.get("display_name", "").lower().strip()
        if not product_name:
            continue
        if listing_name == product_name or listing_name in product_name or product_name in listing_name:
            return product

    return None


def merge_listing_with_data_product(bq_listing: dict, data_product: dict) -> dict:
    """
    Merge a BQ Analytics Hub listing with its matched Data Product catalog entry.

    Strategy
    --------
    - All BQ listing fields are preserved unchanged.
    - Fields present only in the data product are collected under
      ``data_product_unique_fields``.
    - Fields present in both sources (display_name, description) that differ
      are surfaced under ``conflicting_fields`` so callers can decide which to
      present; the BQ value is kept as the primary value.
    - The data product's resource name is stored as ``data_product_name``.

    Returns:
        Merged dict summarising information from both sources.
    """
    merged = dict(bq_listing)
    merged["data_product_name"] = data_product.get("name")

    unique_to_dp: dict = {}
    conflicts: dict = {}

    for key, dp_value in data_product.items():
        if key == "name":
            continue  # already stored as data_product_name

        if key not in bq_listing:
            unique_to_dp[key] = dp_value
        elif key in _SHARED_FIELDS:
            bq_value = bq_listing.get(key)
            if dp_value and bq_value != dp_value:
                conflicts[key] = {"bq_value": bq_value, "dp_value": dp_value}

    merged["data_product_unique_fields"] = unique_to_dp
    if conflicts:
        merged["conflicting_fields"] = conflicts

    return merged


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _is_data_product(entry) -> bool:
    """Return True when the Dataplex entry represents a data product."""
    entry_type: str = getattr(entry, "entry_type", "") or ""
    return "data-product" in entry_type.lower() or "dataproduct" in entry_type.lower()


def _normalize_entry(entry) -> dict:
    """Convert a Dataplex Entry protobuf object to a plain dict."""
    aspects = _extract_aspects(getattr(entry, "aspects", {}) or {})

    create_time = getattr(entry, "create_time", None)
    update_time = getattr(entry, "update_time", None)

    base = {
        "name": entry.name,
        "display_name": entry.display_name,
        "description": entry.description,
        "entry_type": entry.entry_type,
        "create_time": create_time.isoformat() if create_time else None,
        "update_time": update_time.isoformat() if update_time else None,
    }
    base.update(aspects)
    return {k: v for k, v in base.items() if v is not None}


def _extract_aspects(aspects: dict) -> dict:
    """
    Extract well-known metadata from a Dataplex entry's aspects map.

    Each value in ``aspects`` is a ``dataplex_v1.Aspect`` whose ``.data``
    attribute is a ``google.protobuf.Struct`` (behaves like a dict).
    """
    extracted: dict = {}

    for aspect_type, aspect in aspects.items():
        data: dict = dict(getattr(aspect, "data", {}) or {})

        if "data-product-metadata" in aspect_type:
            extracted.update({
                "owner_team": data.get("ownerTeam"),
                "domain": data.get("domain"),
                "data_classification": data.get("dataClassification"),
                "contact_email": data.get("contactEmail"),
            })
        elif "data-product-status" in aspect_type:
            extracted["status"] = data.get("stage")
            extracted["sla_tier"] = data.get("slaTier")
            extracted["update_frequency"] = data.get("updateFrequency")
        elif "data-product-exchange" in aspect_type:
            extracted["documentation_url"] = data.get("documentationUrl")
            extracted["linked_resources"] = data.get("linkedResources", [])
        elif "overview" in aspect_type:
            # Only use as fallback if no description was set on the entry itself
            if not extracted.get("description"):
                extracted["description"] = data.get("details")

    return {k: v for k, v in extracted.items() if v is not None}

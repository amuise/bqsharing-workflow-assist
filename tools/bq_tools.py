from google.cloud import bigquery_data_exchange_v1beta1
from google.api_core import exceptions
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def search_listings(query: str, project_id: str, location: str = "US") -> list[dict]:
    """
    Searches for listings in BigQuery Analytics Hub.

    Args:
        query: The search query string.
        project_id: The Google Cloud Project ID.
        location: The location of the data exchange (default: "US").

    Returns:
        A list of dictionaries representing the found listings.
    """
    client = bigquery_data_exchange_v1beta1.AnalyticsHubServiceClient()
    
    # Construct the parent resource
    parent = f"projects/{project_id}/locations/{location}"
    
    # Note: The actual API doesn't have a direct "search" method like a search engine. 
    # We typically list data exchanges and then list listings within them, 
    # OR if using the specialized search API if enabled.
    # For this implementation, we will assume we are listing from all exchanges in the location
    # and filtering client-side or using the `filter` parameter if supported.
    # However, `list_listings` requires a specific data exchange.
    # To search *across* exchanges, we first list exchanges.
    
    results = []
    
    try:
        # 1. List Data Exchanges
        request = bigquery_data_exchange_v1beta1.ListDataExchangesRequest(parent=parent)
        page_result = client.list_data_exchanges(request=request)
        
        for exchange in page_result:
            # 2. List Listings in each Exchange
            listings_request = bigquery_data_exchange_v1beta1.ListListingsRequest(
                parent=exchange.name
            )
            listings_page = client.list_listings(request=listings_request)
            
            for listing in listings_page:
                # Basic case-insensitive search on title/description
                if query.lower() in listing.display_name.lower() or \
                   (listing.description and query.lower() in listing.description.lower()):
                    
                    results.append({
                        "name": listing.name,
                        "display_name": listing.display_name,
                        "description": listing.description,
                        "data_exchange": exchange.display_name,
                        "listing_id": listing.name.split("/")[-1],
                        "project_id": project_id,
                        "location": location,
                        "exchange_id": exchange.name.split("/")[-1]
                    })
                    
    except exceptions.GoogleAPICallError as e:
        logger.error(f"Error searching listings: {e}")
        return []

    return results

def subscribe_listing(listing_name: str, destination_dataset: str, project_id: str, location: str = "US") -> str:
    """
    Subscribes to a listing in BigQuery Analytics Hub.

    Args:
        listing_name: The full resource name of the listing (e.g., projects/.../listings/...).
        destination_dataset: The ID of the destination dataset to create/use.
        project_id: The Google Cloud Project ID where the subscription will be created.
        location: The location for the subscription dataset.

    Returns:
        The resource name of the subscription, or error message.
    """
    client = bigquery_data_exchange_v1beta1.AnalyticsHubServiceClient()
    
    try:
        # The API requires specifying the destination dataset.
        # We assume the destination dataset reference.
        
        destination_dataset_ref = {
            "dataset_reference": {
                "dataset_id": destination_dataset,
                "project_id": project_id
            },
            "location": location
        }

        request = bigquery_data_exchange_v1beta1.SubscribeListingRequest(
            name=listing_name,
            destination_dataset=destination_dataset_ref
        )
        
        response = client.subscribe_listing(request=request)
        logger.info(f"Subscribed to {listing_name}. Result: {response}")
        return f"Successfully subscribed! Data is available in dataset: {destination_dataset}"

    except exceptions.GoogleAPICallError as e:
        logger.error(f"Error subscribing to listing: {e}")
        return f"Failed to subscribe: {e}"

def get_listing_url(listing_name: str, project_id: str) -> str:
    """
    Generates the Google Cloud Console URL for a listing.
    
    Args:
        listing_name: Full resource name.
        project_id: Project ID.
        
    Returns:
        URL string.
    """
    # Format: https://console.cloud.google.com/bigquery/analytics-hub/exchanges/{exchange_id}/listings/{listing_id}?project={project_id}
    try:
        parts = listing_name.split("/")
        # projects/{project}/locations/{location}/dataExchanges/{exchange}/listings/{listing}
        location = parts[3]
        exchange_id = parts[5]
        listing_id = parts[7]
        
        return f"https://console.cloud.google.com/bigquery/analytics-hub/locations/{location}/exchanges/{exchange_id}/listings/{listing_id}?project={project_id}"
    except IndexError:
        return "https://console.cloud.google.com/bigquery/analytics-hub"

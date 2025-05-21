import azure.functions as func
import logging
import os
import csv
from azure.eventhub import EventHubProducerClient, EventData
from azure.storage.blob import BlobServiceClient
from urllib.parse import urlparse

# Set up Blob Storage client
blob_conn_str = os.environ["BLOB_CONNECTION_STRING"]
blob_service_client = BlobServiceClient.from_connection_string(blob_conn_str)

# Set up Event Hub client
EVENTHUB_CONN_STR = os.environ["EVENTHUB_SEND_CONNECTION_STRING"]
EVENTHUB_NAME = os.environ.get("EVENTHUB_NAME", "realtimehub")
producer = EventHubProducerClient.from_connection_string(
    conn_str=EVENTHUB_CONN_STR, eventhub_name=EVENTHUB_NAME
)

bp = func.Blueprint()  # Create a blueprint for the function


@bp.function_name(name="BlobEventGridTrigger")
@bp.event_grid_trigger(arg_name="event")
def process_event(event: func.EventGridEvent):
    logging.info(f"Received Event Grid event: {event.id}")

    try:
        # Extract the blob URL
        data = event.get_json()
        blob_url = data.get("url")
        logging.info(f"Blob URL: {blob_url}")

        if not blob_url:
            raise ValueError("Event data missing 'url' field.")

        # Parse the URL to get container name and blob path
        parsed_url = urlparse(blob_url)
        path_parts = parsed_url.path.lstrip("/").split("/", 1)
        container_name = path_parts[0]
        blob_name = path_parts[1]

        # Get the blob client and download content
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        blob_data = blob_client.download_blob().readall().decode("utf-8")

        # Process CSV data
        reader = csv.DictReader(blob_data.splitlines())
        events = []
        for row in reader:
            review = row.get("text")
            if review and len(review.strip()) > 0:
                events.append(EventData(review.strip()))

        # Send to Event Hub
        if events:
            with producer:
                producer.send_batch(events)
            logging.info(f"Sent {len(events)} reviews to Event Hub.")
        else:
            logging.warning("No valid reviews found in blob.")

    except Exception as e:
        logging.error(f"Failed to process blob event: {e}")

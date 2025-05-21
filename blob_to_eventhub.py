import azure.functions as func
import logging
import os
import csv
from azure.eventhub import EventHubProducerClient, EventData
from azure.storage.blob import BlobServiceClient

# Set up Blob Storage client
blob_conn_str = os.environ["BLOB_CONNECTION_STRING"]
blob_container = os.environ["BLOB_CONTAINER_NAME"]
blob_service_client = BlobServiceClient.from_connection_string(blob_conn_str)
container_client = blob_service_client.get_container_client(blob_container)

# Set up Event Hub client
# Use the same connection string as your EventHub trigger, but with send permissions
EVENTHUB_CONN_STR = os.environ["EVENTHUB_SEND_CONNECTION_STRING"]
EVENTHUB_NAME = os.environ.get("EVENTHUB_NAME", "realtimehub")

producer = EventHubProducerClient.from_connection_string(
    conn_str=EVENTHUB_CONN_STR, eventhub_name=EVENTHUB_NAME
)

bp = func.Blueprint()  # Create a blueprint for the blob trigger


@bp.function_name(name="BlobToEventHub")  # Name of the function
@bp.blob_trigger(
    arg_name="myblob",
    path=f"{blob_container}/{{name}}",
    connection="BLOB_CONNECTION_STRING",
)
def blob_to_eventhub(myblob: func.InputStream):
    logging.info(f"Processing blob: {myblob.name}, Size: {myblob.length} bytes")
    try:
        # Read CSV from blob
        content = myblob.read().decode("utf-8")
        reader = csv.DictReader(content.splitlines())
        events = []
        for row in reader:
            # Use 'text' column instead of 'reviews'
            review = row.get("text")
            if review and len(review.strip()) > 0:
                events.append(EventData(review.strip()))
        if events:
            # Send all reviews as separate events
            with producer:
                producer.send_batch(events)
            logging.info(f"Sent {len(events)} reviews to Event Hub.")
        else:
            logging.warning("No valid reviews found in blob.")
    except Exception as e:
        logging.error(f"Error processing blob to Event Hub: {e}")

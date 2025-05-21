import azure.functions as func
import logging
import os
import json
import re
from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential
from azure.storage.blob import BlobServiceClient
from blob_to_eventhub import bp  # Import the blueprint from the previous code

app = func.FunctionApp()
app.register_blueprint(bp)  # Register the blueprint for the blob trigger

# Set up Azure Text Analytics client
text_analytics_endpoint = os.environ["TEXT_ANALYTICS_ENDPOINT"]
text_analytics_key = os.environ["TEXT_ANALYTICS_KEY"]
text_analytics_client = TextAnalyticsClient(
    endpoint=text_analytics_endpoint, credential=AzureKeyCredential(text_analytics_key)
)

# Set up Azure Blob Storage client for input (if needed)
blob_conn_str = os.environ["BLOB_CONNECTION_STRING"]
blob_service_client = BlobServiceClient.from_connection_string(blob_conn_str)
# Output container for results
output_container = os.environ.get("OUTPUT_CONTAINER_NAME", "output")
output_container_client = blob_service_client.get_container_client(output_container)


def is_valid_review(text):
    # Remove leading/trailing whitespace
    text = text.strip()
    # Check if text is empty or too short
    if not text or len(text) < 10:
        return False
    # Check for code-like patterns (e.g., curly braces, semicolons, import, def, class)
    code_patterns = [r"\bimport\b", r"\bdef\b", r"\bclass\b", r"[{};]", r"<.*?>"]
    if any(re.search(pattern, text) for pattern in code_patterns):
        return False
    # Check if text is mostly printable characters
    if sum(c.isprintable() for c in text) / len(text) < 0.9:
        return False
    return True


@app.event_hub_message_trigger(
    arg_name="azeventhub",
    event_hub_name="realtimehub",
    connection="sentimentsEventHubs_listen_EVENTHUB",
)
def eventhub_trigger(azeventhub: func.EventHubEvent):
    logging.info(
        "Python EventHub trigger processed an event: %s",
        azeventhub.get_body().decode("utf-8"),
    )
    try:
        review_text = azeventhub.get_body().decode("utf-8").strip()
        if not is_valid_review(review_text):
            logging.warning("Review text is not valid or is likely code/gibberish.")
            return
        # Call Text Analytics for sentiment
        response = text_analytics_client.analyze_sentiment([review_text])[0]
        sentiment_result = {
            "review": review_text,
            "sentiment": response.sentiment,
            "confidence_scores": {
                "positive": response.confidence_scores.positive,
                "neutral": response.confidence_scores.neutral,
                "negative": response.confidence_scores.negative,
            },
            "id": azeventhub.sequence_number,
        }
        # Store result in output Blob Storage
        blob_name = f"sentiment_{azeventhub.sequence_number}.json"
        output_container_client.upload_blob(
            blob_name, json.dumps(sentiment_result), overwrite=True
        )
        logging.info(f"Sentiment result stored in output blob: {blob_name}")
    except Exception as e:
        logging.error(f"Error processing event: {e}")

import logging
import azure.functions as func
from azure.servicebus import ServiceBusClient, ServiceBusMessage
import pandas as pd
import io
import os
import json
#from dotenv import load_dotenv
#load_dotenv()


logging.basicConfig(level=logging.INFO)

def main(blob: func.InputStream):
    logging.info(f"Processing blob: Name={blob.name}, Size={blob.length} bytes")

    # Read CSV file into a pandas DataFrame
    try:
        df = pd.read_csv(io.BytesIO(blob.read()))
        logging.info(f"CSV read successfully. Rows: {df.shape[0]}, Columns: {df.shape[1]}")
    except Exception as e:
        logging.error(f"Failed to read CSV: {e}")
        return
    

    # Create summary (example: number of rows and columns)
    summary = {
        "fileName": blob.name,
        "numRows": df.shape[0],
        "numColumns": df.shape[1],
        "columns": df.columns.tolist()
        #"columns": [col for col in df.columns if col in ["Name", "Age"]]
    }
    logging.info("Preparing to send message to Service Bus...")

    # Serialize summary message
    #message_body = str(summary)
    message_body = json.dumps(summary)
    logging.info(f"Sending message to Service Bus: {message_body}")

    # Service Bus config from environment variables
    #servicebus_conn_str = os.getenv("vishalservicebus_RootManageSharedAccessKey_SERVICEBUS")
    servicebus_connection_str = os.environ['SERVICE_BUS_CONNECTION']
    queue_name = os.environ['SERVICE_BUS_QUEUE']

    try:
        with ServiceBusClient.from_connection_string(servicebus_connection_str) as client:
            sender = client.get_queue_sender(queue_name=queue_name)
            with sender:
                msg = ServiceBusMessage(message_body)
                sender.send_messages(msg)
                logging.info("Message sent successfully.")
    except Exception as e:
        logging.error(f"Failed to send message to Service Bus: {e}")

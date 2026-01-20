from azure.eventhub import EventHubProducerClient
import os

def get_eventhub_producer():
    conn_str = os.getenv("EVENTHUB_CONNECTION_STRING")
    hub_name = os.getenv("EVENTHUB_NAME")

    return EventHubProducerClient.from_connection_string(
        conn_str=conn_str,
        eventhub_name=hub_name
    )

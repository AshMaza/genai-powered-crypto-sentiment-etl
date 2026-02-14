import json
from azure.eventhub import EventHubProducerClient, EventData

def send_to_eventhub(eventhub_con, eventhub_name, event_envelopes):
    conn_str = eventhub_con
    hub_name = eventhub_name

    producer = EventHubProducerClient.from_connection_string(
        conn_str=conn_str,
        eventhub_name=hub_name
    )

    if len(event_envelopes) > 0:
        with producer:
            batch = producer.create_batch()

            for event in event_envelopes:
                event_data = EventData(json.dumps(event))
                try:
                    batch.add(event_data)
                except ValueError:
                    producer.send_batch(batch)
                    batch = producer.create_batch()
                    batch.add(event_data)
                except Exception as e:
                    print(f"Error adding event to batch: {e}")
                    break

            if len(batch) > 0:
                producer.send_batch(batch)

        print(f"\nSent {len(event_envelopes)} events to Event Hub: {hub_name}")

    else:
        print("No news found to send to Event Hub.")

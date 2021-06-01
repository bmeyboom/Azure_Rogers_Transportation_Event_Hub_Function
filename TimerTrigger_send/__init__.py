# Issues:
# Confusing output data in Data Lake

import datetime
import logging
import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
import time
from BCTWSConnection import BCTWSConnection

import azure.functions as func

eventhub_connection_str = "Endpoint=sb://transportcloudingest.servicebus.windows.net/;SharedAccessKeyName=FunctionBinding;SharedAccessKey=r1BjgrtmF55pA6lfazUzN9sYL52C4atXKpQaEf9M7cw="
eventhub_name = "bluecityeventhub"


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    # loop = asyncio.get_event_loop()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run())


async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(conn_str=eventhub_connection_str, eventhub_name=eventhub_name)
    # producer.send()
    async with producer:
        # Create a batch. 
        # Connect to Blue City API
        query = BCTWSConnection("BCT_3D_5G_0103002", "t6oSb-MepdXvDm88rjv-3UybUFprW-cX58CSO9DmmxkimwC5Qj01vwuVwT_1X95AkjBRfb53PunWpQDN")
        event_data_batch = await producer.create_batch()

        # Add events to the batch
        # t_end = time.time() + 60 * 5 # run for 5 minutes (until next function trigger)
        t_end = time.time() + 30 # run for 30 seconds (until next function trigger) -> need to change in function.json cron expression

        can_add = True # 
        while time.time() < t_end and can_add:
            try:
                event_data_batch.add(EventData(body=query.get())) # query the BCTWSConnection
            except ValueError:
                can_add = False
        
        

        logging.info(event_data_batch)
        query.terminate() # terminate stream from Blue City API

        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)

    # # loop = asyncio.get_event_loop()
    # loop = asyncio.new_event_loop()
    # asyncio.set_event_loop(loop) 
    # loop.run_until_complete(run())
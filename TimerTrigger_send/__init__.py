# Issues:
# Confusing output data in Data Lake
# https://stackoverflow.com/questions/62289303/azure-functions-python-send-eventdata-messages-with-properties-to-event-hub-ou 
import datetime
import logging
from multiprocessing import Event
# import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
# import time
from BCTWSConnection import BCTWSConnection

import azure.functions as func

# connect to BlueCity API
BLUE_CITY_CONNECTION = BCTWSConnection("BCT_3D_5G_0103002", "t6oSb-MepdXvDm88rjv-3UybUFprW-cX58CSO9DmmxkimwC5Qj01vwuVwT_1X95AkjBRfb53PunWpQDN")


# eventhub_connection_str = "Endpoint=sb://transportcloudingest.servicebus.windows.net/;SharedAccessKeyName=FunctionBinding;SharedAccessKey=r1BjgrtmF55pA6lfazUzN9sYL52C4atXKpQaEf9M7cw="
# eventhub_name = "bluecityeventhub"


def main(mytimer: func.TimerRequest, outputEventHubMessage: func.Out[str]) -> None:
    # get a value from the API
    frame = BLUE_CITY_CONNECTION.get()
    logging.info(str('raw frame: ' + frame))
    
    # push the frame to Event Hub
    # replace '-' so that can be parse correctly
    frame.replace('-', '/')
    outputEventHubMessage.set(str("[" + str(EventData(frame)) + "]"))
    
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
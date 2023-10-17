import json
import asyncio
from datetime import datetime

from pyensign.ensign import Ensign
from pyensign.api.v1beta1.ensign_pb2 import Nack
from pyensign.events import Event

from river import compose
from river import cluster
from river import stream
from river import preprocessing
from river.cluster import STREAMKMeans


async def handle_ack(ack):
    ts = datetime.fromtimestamp(ack.committed.seconds + ack.committed.nanos / 1e9)
    print(ts)

async def handle_nack(nack):
    print(f"Could not commit event {nack.id} with error {nack.code}: {nack.error}")

class WeatherSubscriber:
    """
    WeatherSubscriber subscribes to an Ensign stream that the WeatherPublisher is
    writing new weather reports to, then clusters by city, and publishes the results 
    to the "city-cluster-results" topic.
    """

    def __init__(self, topic="weather_forcasts-JSON", pub_topic="city-cluster-results"):

        self.topic = topic
        self.pub_topic = pub_topic
        
        keys = self._load_keys()

        self.ensign = Ensign(
            client_id=keys["ClientID"],
            client_secret=keys["ClientSecret"]
        )

        self.initialize_model()

    def _load_keys(self):
        try:
            f = open("client.json")
            return json.load(f)
        except Exception as e:
            raise OSError(f"unable to load Ensign API keys from file: ", e)
        
    def run(self):
        """
        Run the subscriber forever.
        """
        asyncio.run(self.subscribe())
    
    def initialize_model(self):
        """
        Initialize a river clustering model
        Update the parameters as needed for your project
        """
        self.model = STREAMKMeans(chunk_size=3, n_clusters=2, halflife=0.5, sigma=1.5, seed=0)
        
        #self.model = compose.Pipeline(
        #    preprocessing.StandardScaler(),
        #    STREAMKMeans(chunk_size=3, n_clusters=2, halflife=0.5, sigma=1.5, seed=0)
        #    )
    
    async def handle_event(self, event):
        """
        Decode and ack the event.
        """
        try:
            data = json.loads(event.data)
        except json.JSONDecodeError:
            print("Received invalid JSON in event payload:", event.data)
            await event.nack(Nack.Code.UNKNOWN_TYPE)
            return
        
        if data["precipitation"]["value"] is None:
            data["precipitation"]["value"] = 0

        '''Replace "data" with "x" to limit fields being fed to model:
        x = [data["temperature"], data["precipitation"]["value"]]
        '''
        
        if len(self.model.centers) > 1:
            cluster = self.model.predict_one(data)
#            print(cluster)
        self.model = self.model.learn_one(data)
        
        #cluster = 1 #### Dummy cluster to remove once model is working

#        data.update(cluster = cluster)

#line 94, in handle_event
#    data.update(cluster = cluster)
#                          ^^^^^^^
#UnboundLocalError: cannot access local variable 'cluster' where it is not associated with a value



#line 88, in handle_event
#    cluster = self.model.predict_one(data)
#              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#  File "C:\Users\sveldman\AppData\Local\anaconda3\Lib\site-packagester\streamkmeans.py", line 114, in predict_one
#    return min(self.centers, key=get_distance)
#                ^^^^'center' also threw an error in this spot when I was using pipeline for standard scaler.

        print("New cluster results available:", data)

        event = Event(json.dumps(data).encode("utf-8"), mimetype="application/json")
        await self.ensign.publish(self.pub_topic, event, on_ack=handle_ack, on_nack=handle_nack)

    async def subscribe(self):
        """
        Subscribe to the weather report topic and parse the events.
        """
        id = await self.ensign.topic_id(self.topic)
        async for event in self.ensign.subscribe(id):
            await self.handle_event(event)
            
if __name__ == "__main__":
    subscriber = WeatherSubscriber()
    subscriber.run()
  
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

    def __init__(self, topic="weather-forecasts", pub_topic="city-clusters"):

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
        Initialize river clustering model and apply standard scaler with Pipeline.
        Parameters of the model should be tuned to produce clusters most useful to individual applications/use cases.
        """
        
        self.model = compose.Pipeline(
            preprocessing.StandardScaler(),
            STREAMKMeans(chunk_size=3, n_clusters=5, halflife=0.5, sigma=1.5, seed=0)
            )
    
    async def handle_event(self, event):
        """
        Decode and ack the event, input event into the clustering model:
        """
        try:
            data = json.loads(event.data)
        except json.JSONDecodeError:
            print("Received invalid JSON in event payload:", event.data)
            await event.nack(Nack.Code.UNKNOWN_TYPE)
            return
        
        if data["precipitation"]["value"] is None:
            data["precipitation"]["value"] = 0

        '''
        Here we use 'x' to control which fields from the forecast are being fed to the model:
        '''
        x = {"temperature": data["temperature"], "precipitation": data["precipitation"]["value"]}

        '''
        The first time the model is run it will be missing attributes and need to be treated differently:
        '''
        try:
            if len(self.model.centers) > 1:
                cluster = self.model.predict_one(x)
                data.update(cluster = cluster)
            self.model = self.model.learn_one(x)
        except AttributeError:
            self.model = self.model.learn_one(x)
            cluster = self.model.predict_one(x)
            data.update(cluster = cluster)

        print("New cluster results available:", data)

        '''
        Publish data to new topic once it has been assigned a cluster:
        '''
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
  
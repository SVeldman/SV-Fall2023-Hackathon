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

    async def handle_event(self, event):
        """
        Decode and ack the event, assign classification,
        """
        try:
            data = json.loads(event.data)
        except json.JSONDecodeError:
            print("Received invalid JSON in event payload:", event.data)
            await event.nack(Nack.Code.UNKNOWN_TYPE)
            return
        
                

        if data["precipitation"]["value"] is None:
            data["precipitation"]["value"] = 0

        '''Replace "data" with "x" to limit fields being fed into model'''
        X = [data["temperature"], data["precipitation"]["value"]]


        
##### Code below was developed and tested with a dictionary of values as input;
##### Both versions are technically working, but are assigning everything to cluster 1


#        model = compose.Pipeline(
#            preprocessing.StandardScaler(),
#            cluster.KMeans(n_clusters=4, halflife=0.1, sigma=3, seed=42)
#            )

####line 80, in handle_event
    ####cluster.KMeans(n_clusters=4, halflife=0.1, sigma=3, seed=42)
    ####^^^^^^^
####UnboundLocalError: cannot access local variable 'cluster' where it is not associated with a value

#### OR ####

#### line 80, in handle_event
    ####KMeans(n_clusters=4, halflife=0.1, sigma=3, seed=42) 
    ####^^^^^^
####NameError: name 'KMeans' is not defined. Did you mean: 'k_means'?

####        for i, (x, _) in enumerate(stream.iter_array(X)):
####            k_means = model.learn_one(x)
####            cluster = model.predict_one(x)
####            #clusters.append(cluster)
####            data.update(cluster = cluster)
            

        cluster = 1 #### Dummy cluster to remove once model is working
        data.update(cluster = cluster) 
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
    
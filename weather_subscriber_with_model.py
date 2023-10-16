import json
import asyncio

from pyensign.ensign import Ensign
from pyensign.api.v1beta1.ensign_pb2 import Nack
from river import compose
from river import cluster
from river import stream
from river import preprocessing

class WeatherSubscriber:
    """
    WeatherSubscriber subscribes to an Ensign stream that the WeatherPublisher is
    writing new weather reports to at some regular interval.
    """

    def __init__(self, topic="weather_forcasts-JSON"):
        """
        Initialize the WeatherSubscriber, which will allow a data consumer to subscribe
        to the topic that the publisher is writing weather data reports to

        Parameters
        ----------
        topic : string, default: "noaa-reports-json"
            The name of the topic you wish to subscribe to.
        """
        self.topic = topic
        
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
        Decode and ack the event.
        """
        try:
            data = json.loads(event.data)
        except json.JSONDecodeError:
            print("Received invalid JSON in event payload:", event.data)
            await event.nack(Nack.Code.UNKNOWN_TYPE)
            return
        
#####
        result = data

        if data["precipitation"]["value"] is None:
            data["precipitation"]["value"] = 0

        X = [data["temperature"], data["precipitation"]["value"]]

        model = compose.Pipeline(
            preprocessing.StandardScaler(),
            cluster.KMeans(n_clusters=4, halflife=0.1, sigma=3, seed=42)
            )

##### Code for the kmeans model was tested with a list of values as input;
##### I am not sure how to adjust for one set of valued coming in at a time.
        for i, (x, _) in enumerate(stream.iter_array(X)):
            k_means = model.learn_one(x)
            cluster = model.predict_one(x)
            #clusters.append(cluster)
            data.update(cluster = cluster)
#####
        print("New cluster results available:", data)
        await event.ack()

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


# now add publisher to publish to "“weather-cluster-model” topic"    
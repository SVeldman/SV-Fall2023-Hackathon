from pyensign.ensign import Ensign

import os
import json
import asyncio
import warnings

from datetime import datetime

import requests
from requests import HTTPError, ConnectionError
from pyensign.events import Event
from pyensign.ensign import Ensign

warnings.filterwarnings("ignore")

ME = "(https://rotational.io/data-playground/noaa/, veldman@uchicago.edu)"

class WeatherPublisher:
    """
    WeatherPublisher queries an API for weather updates and publishes events to Ensign.
    """

    def __init__(self, topic="weather-forecasts", interval=60, user=ME):
        """
        Initialize a WeatherPublisher by specifying a topic, locations, and other user-
        defined parameters.

        Parameters
        ----------
        topic : string, default: "noaa-reports-json"
            The name of the topic you wish to publish to. If the topic doesn't yet
            exist, Ensign will create it for you. Tips on topic naming conventions can
            be found at https://ensign.rotational.dev/getting-started/topics/

        interval : int, default: 60
            The number of seconds to wait between API calls so that you do not anger
            the weather API gods

        locations : dict
            A dictionary expressing the locations to retrieve weather details for.
            Note that these should all be in the USA since NOAA is located in the US :)

        user : str
            When querying the NOAA API, as a courtesy, they like you to identify your
            app and contact info (aka User Agent details)
        """

        self.topic = topic
        self.interval = interval
        self.locations = self._load_cities()
        self.url = "https://api.weather.gov/points/"
        self.user = {"User-Agent": user}
        self.datatype = "application/json"

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
    
    def _load_cities(self):
        cities = dict()
        try:
            f = open(os.path.join("cities.json"))
            json_lines = json.load(f)
            for l in json_lines:
                cities[l["city"]] = {
                    "city": str(l["city"]),
                    "lat": str(l["latitude"]),
                    "long": str(l["longitude"])
                }
            return cities
        except Exception as e:
            raise OSError(f"unable to load cities from file: ", e)

    async def print_ack(self, ack):
        """
        Enable the Ensign server to notify the Publisher the event has been acknowledged

        This is optional for you, but can be very helpful for communication in
        asynchronous contexts!
        """
        ts = datetime.fromtimestamp(ack.committed.seconds + ack.committed.nanos / 1e9)
        print(f"Event committed at {ts}")

    async def print_nack(self, nack):
        """
        Enable the Ensign server to notify the Publisher the event has NOT been
        acknowledged

        This is optional for you, but can be very helpful for communication in
        asynchronous contexts!
        """
        print(f"Event was not committed with error {nack.code}: {nack.error}")

    def compose_query(self, location):
        """
        Combine the base URI with the lat/long query params

        Parameters
        ----------
        location : dict
            A dictionary expressing a locations to retrieve weather details for.
            Note that it should all be in the USA since NOAA is located in the US :)
            For example: {"lat": "64.7511", "long": "-147.3494"}
        """
        lat = location.get("lat", None)
        long = location.get("long", None)
        if lat is None or long is None:
            raise Exception("unable to parse latitude/longitude from location")

        return self.url + lat + "," + long

    def run(self):
        """
        Run the publisher forever.
        """
        asyncio.run(self.recv_and_publish())

    async def recv_and_publish(self):
        """
        At pre-defined interval (`self.interval`), ping the api.weather.com to get 
        weather reports for the `self.locations`.

        NOTE: this requires 2 calls to the NOAA API, per location:
            - the first request provides a lat/long and retrieves forecast URL
            - the second request provides the forecast URL and gets forecast details

        Publish report data to the `self.topic`
        """
        await self.ensign.ensure_topic_exists(self.topic)

        while True:
            for location in self.locations.values():
                # Call the API for each location
                query = self.compose_query(location)

                # If successful, the initial response returns a link used to retrieve the full hourly forecast
                try:
                    response = requests.get(query).json()
                except ConnectionError as e:
                    print(e)
                    continue
                except HTTPError as e:
                    print(e)
                    continue

                forecast_url = self.parse_forecast_link(response)

                try:
                    forecast = requests.get(forecast_url).json()
                except ConnectionError as e:
                    print(e)
                    continue
                except HTTPError as e:
                    print(e)
                    continue
                except Exception as e:
                    print(e)
                    continue

                # Define "city" variable so that we can package city name with forecast data when we unpack the response
                city = location.get("city", None)

                # After we retrieve and unpack the full hourly forecast, publish each period of the forecast as a new event
                try:
                    events = self.unpack_noaa_response(forecast, city)
                except Exception as e:
                    print(e)
                    continue

                for event in events:
                    await self.ensign.publish(
                        self.topic,
                        event,
                        on_ack=self.print_ack,
                        on_nack=self.print_nack,
                    )
            await asyncio.sleep(self.interval)

    def parse_forecast_link(self, message):
        """
        Parse a preliminary forecast response from the NOAA API to get a forecast URL

        Parameters
        ----------
        message : dict
            JSON formatted response from the NOAA API containing a forecast URL

        Returns
        -------
        forecast_link : string
            Specific API-generated URL with the link to get the detailed forecast for
            the requested location
        """
        properties = message.get("properties", None)
        if properties is None:
            raise Exception("unexpected response from api call, no properties")

        forecast_link = properties.get("forecast", None)
        if forecast_link is None:
            #raise Exception("unexpected response from api call, no forecast")
            print("unexpected response from api call, no forecast")
            '''
            This is not the most elegant way to handle this exception, but after adding the code
            in the above section this exception was still crashing the publisher when it came up.
            For now this fix allows the publisher to continue to run and prints the error for us to see.
            '''
        return forecast_link

    def unpack_noaa_response(self, message, city):
        """
        Convert a message from the NOAA API to potentially multiple Ensign events and yield each.

        Parameters
        ----------
        message : dict
            JSON formatted response from the NOAA API containing forecast details
        """
        properties = message.get("properties", None)
        if properties is None:
            raise Exception("unexpected response from forecast request, no properties") #############

        periods = properties.get("periods", None)
        if periods is None:
            raise Exception("unexpected response from forecast request, no periods") #################

        for period in periods:
            data = {
                "city": city,
                "name": period.get("name", None),
                "summary": period.get("shortForecast", None),
                "temperature": period.get("temperature", None),
                "units": period.get("temperatureUnit", None),
                "precipitation": period.get("probabilityOfPrecipitation", None), 
                "dewpoint": period.get("dewpoint", None),
                "humidity": period.get("relativeHumidity", None),
                "windspeed": period.get("windSpeed", None),
                "daytime": period.get("isDaytime", None),
                "start": period.get("startTime", None),
                "end": period.get("endTime", None),
            }

            yield Event(json.dumps(data).encode("utf-8"), mimetype=self.datatype)


if __name__ == "__main__":
    publisher = WeatherPublisher()
    publisher.run()
    

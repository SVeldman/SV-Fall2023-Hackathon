# SV-Fall2023-Hackathon

The topic for this hackathon was building real-time machine learning models using the publisher/subscriber (pub/sub) paradigm. I utilized Rotational Labs’ Ensign tool to access the national weather service’s public API. Using a variant of the K-Means algorithm designed for working with data streams, I built a model to provide real-time market segmentation of the top 1,000 most populace US cities based on current weather forecast. 

Video Presentation of this Project:
    https://youtu.be/Ip0-NbHMXyk

Source for Cities JSON file:
    "1000 Largest US Cities By Population With Geographic Coordinates, in JSON"
    https://gist.github.com/Miserlou/c5cd8364bf9b2420bb29

Example Code Referenced:
    Rotational Labs GitHub Weather Examples:
    https://github.com/rotationalio/ensign-examples/tree/main/python/weather_data

    Rotational Labs GitHub - River Sentiment Analysis Example:
    https://github.com/rotationalio/online-model-examples/blob/main/river/river_sentiment_analysis.py

    Rotational Labs GitHub - River Clustering Example:
    https://github.com/rotationalio/online-model-examples/blob/main/river/river_clustering.py

    River Library Documentation - Kmeans:
    https://riverml.xyz/dev/api/cluster/KMeans/

    River Library Documentation - STREAMKmeans:
    https://riverml.xyz/dev/api/cluster/STREAMKMeans/

    River Library Documentation - Pipelines:
    https://riverml.xyz/0.19.0/recipes/pipelines/


from river import compose
from river import cluster
from river import stream
from river import preprocessing
import json

# Do I need to add River to the requirements.txt file?


##### these lines will go away, and "raw_data" will be replaced with "data" from subscriber
raw_data = open("synthetic_events.json")
raw_lines = json.load(raw_data)
#####

results = dict()
index = 0 ################ this will be replaced with "city" once I figure out how to pull that in
for line in raw_lines:
    results[index] = {
        "start": line["start"],
        "end": line["end"],
        "temp": line["temperature"],
        "precip": line["precipitation"]["value"],
        #"cluster": ""
    }
    index += 1 ################ this goes away once "city" is included


X = []
for city in results:
    if results[city]["precip"] is None:
        results[city]["precip"] = 0
    X.append([results[city]["temp"], results[city]["precip"]])


model = compose.Pipeline(
    preprocessing.StandardScaler(),
    cluster.KMeans(n_clusters=4, halflife=0.1, sigma=3, seed=42)
)

clusters = []
for i, (x, _) in enumerate(stream.iter_array(X)):
    model = model.learn_one(x)
    cluster = model.predict_one(x)
    clusters.append(cluster)

#for i in results[city]["cluster"], clusters:
#    results[city]["cluster"] = clusters

#for i in results[city], clusters:
#    results[city].update(clusters = clusters)

results.update(clusters = clusters)

print(results)

# Now publish to "model" topic

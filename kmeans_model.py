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
for line in raw_lines:
    results[line["city"]] = {
        "temp": line["temperature"],
        "precip": line["precipitation"]#,
        #"cluster": ""
    }

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
    k_means = model.learn_one(x)
    cluster = model.predict_one(x)
    clusters.append(cluster)

#for i in results[city]["cluster"], clusters:
#    results[city]["cluster"] = clusters

#for i in results[city], clusters:
#    results[city].update(clusters = clusters)

results[city].update(clusters = clusters)

print(results)

# Now publish to "model" topic

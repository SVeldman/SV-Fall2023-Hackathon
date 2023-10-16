from river import compose
from river import cluster
from river import stream
from river import preprocessing
import json

# Do I need to add River to the requirements.txt file?

data = open("single_event.json")
result = json.load(data)

if result["precipitation"]["value"] is None:
    result["precipitation"]["value"] = 0

X = [result["temperature"], result["precipitation"]["value"]]

model = compose.Pipeline(
    preprocessing.StandardScaler(),
    cluster.KMeans(n_clusters=4, halflife=0.1, sigma=3, seed=42)
    )

for i, (x, _) in enumerate(stream.iter_array(X)):
    k_means = model.learn_one(x)
    cluster = model.predict_one(x)
    result.update(cluster = cluster)

print(result)


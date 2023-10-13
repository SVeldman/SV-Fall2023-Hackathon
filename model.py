from river import compose
from river import cluster
from river import stream
from river import preprocessing




# Replace X with data from weather topic
X = [
    [1, 0.5], [1, 0.625], [1, 0.75], [1, 1.125], [1, 1.5], [1, 1.75],
    [4, 1.5], [4, 2.25], [4, 2.5], [4, 3], [4, 3.25], [4, 3.5]
]

model = compose.Pipeline(
    preprocessing.StandardScaler(),
    cluster.STREAMKMeans(chunk_size=3, n_clusters=4, halflife=0.5, sigma=1.5, seed=0)
)
#not sure if I am using the pipeline correctly

for x, _ in stream.iter_array(X):
    streamkmeans = model.learn_one(x)

streamkmeans.predict_one({0: 1, 1: 0})
#Not sure what exactly is happening here

#Is this where I publish to the model topic?
# Do I need to publish to model topic, then subscribe to model, publish predictions? Or is it enough to ingest the data from the weather topic, 
# and then publish to a results/prediction topic?



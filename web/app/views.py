# jsonify creates a json representation of the response
from flask import render_template
from app import app
import os.path
import json
from cassandra.cluster import Cluster
from collections import Counter

# load configuration
with open(os.path.dirname(__file__) + "/../../config.json", "r") as f:
    config = json.load(f)

cluster = Cluster([config["DEFAULT"]["DBCLUSTER_PRIVATE_IP"]])
session = cluster.connect(config["CASSANDRA"]["KEYSPACE"])

# demo web page
@app.route("/demo")
def demo():
 return render_template("bar2.html")

# take GET request from bar2.html and return the data to display
@app.route('/update', methods=['GET'])
def update():
    query = "SELECT content FROM others WHERE category = 'tweet_count_web'"
    response = session.execute(query)
    return json.dumps(dict(Counter(json.loads(response[0].content)).most_common()[1:]))
    #return response[0].content


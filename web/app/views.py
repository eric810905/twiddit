# jsonify creates a json representation of the response
from flask import jsonify
from flask import render_template
from app import app
from flask import request

# importing Cassandra modules from the driver we just installed
from cassandra.cluster import Cluster

# Setting up connections to cassandra

cluster = Cluster(['10.0.0.4'])
session = cluster.connect('playground')

@app.route("/demo")
def demo():
 return render_template("bar2.html")

@app.route('/update', methods=['GET'])
def update():
    query = "SELECT content FROM others WHERE category = 'tweet_count_web'"
    response = session.execute(query)
    #return jsonify(response[0].content)
    return response[0].content


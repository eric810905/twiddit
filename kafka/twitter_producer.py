"""This script produce twitter messages to kafka.
"""
from kafka.producer import KafkaProducer
import json
import time
import argparse
import os.path

# load configurations
with open(os.path.dirname(__file__) + "../config.json", "r") as f:
    config = json.load(f)

# parse the arguments if provided in the command
parser = argparse.ArgumentParser(description='Produce twitter streaming.')
parser.add_argument("--kafka-ip-addr", default = config["KAFKA"]["BROKER_IP"],
                    help="ip for kafka")
parser.add_argument("--path", default = config["KAFKA"]["TWITTER_PATH"],
                    help="path of the twitter messages json file.")
args = parser.parse_args()

class Producer(object):
    def __init__(self, addr):
        self.producer = KafkaProducer(bootstrap_servers=addr)

    def produce_msgs(self):
	with open(args.path) as f:
	    content = f.readlines()
	    content = [x.strip() for x in content] 

	while True:
	    for tweet in content:
		tweet_dict = json.loads(tweet)
		if 'text' in tweet_dict and tweet_dict['lang'] == 'en':
	            self.producer.send('twitter',tweet.encode('utf-8'))
		    #time.sleep(0.001)

if __name__ == "__main__":
    ip_addr = args.kafka_ip_addr
    prod = Producer(ip_addr)
    prod.produce_msgs() 

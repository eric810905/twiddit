from pyspark import SparkContext  
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils  
#    json parsing
import json 
import string
from pyspark.sql import SparkSession
import random
import re
from nltk.corpus import stopwords
from cassandra.cluster import Cluster
"""
__all__ = ["StreamingListener"]

class myListener(object):
#    def __init__(self, session):
#        self.session = session
    class Java:
        implements = ["org.apache.spark.streaming.api.java.PythonStreamingListener"]

    def onBatchCompleted(self, batchCompleted):
        pass
        #print(type(batchCompleted))
        #print("WoOoOoF!!save to cassandra")
        #return
"""
english_stopwords = stopwords.words("english")

sc = SparkContext(appName="spark_streaming_kafka")
sc.setLogLevel("WARN")

spark = SparkSession(sc).builder\
                .master("spark://10.0.0.10:7077")\
                .appName("spark_stream")\
                .config("--packages", "com.datastax.spark:spark-cassandra-connector_2.11:2.0.6,org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2")\
                .getOrCreate()

freq_vector_df = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="freq_vector", keyspace="playground")\
    .load()

others_df = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="others", keyspace="playground")\
    .load()

word_count_dict = dict(freq_vector_df.rdd.map(lambda row: (row.word, map(int, row.counts.split()))).collect())
    
subreddit_list = others_df.select("content").where("category = 'subreddits'").collect()
subreddit_list = subreddit_list[0][0].split()

total_count_list = others_df.select("content").where("category = 'total_counts'").collect()
total_count_list = map(int, total_count_list[0][0].split())

def check_database_update():
    cluster = Cluster(['10.0.0.4'])
    session = cluster.connect('playground')

    query = "SELECT content FROM others WHERE category = 'tweet_count'"
    response = session.execute(query)
    tweet_count_dict = json.loads(response[0].content)

    if len(tweet_count_dict) != len(subreddit_list):
        tweet_count_dict = dict(zip(subreddit_list, [0 for _ in range(len(subreddit_list))]))
    
    query = "INSERT INTO others (category, content) VALUES ('tweet_count', '%s')" % (json.dumps(tweet_count_dict))
    session.execute(query)
    session.shutdown()
    return

check_database_update()
print("--finish knowledge setup--")

ssc = StreamingContext(sc, 10) 

#listener = myListener()
#ssc.addStreamingListener(listener)

kafkaStream = KafkaUtils.createStream(ssc, 'ec2-35-161-255-24.us-west-2.compute.amazonaws.com:2181', 'spark-streaming', {'twitter':1}) 

parsed = kafkaStream.map(lambda v: json.loads(v[1])) 

parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()

def get_word_set(tweet_text):
    tweet_text = filter ( lambda x: x in set(string.printable), tweet_text)
    tweet_text = tweet_text.lower()
    word_list = re.findall(r'\w+', tweet_text, flags = re.UNICODE | re.LOCALE)
    important_word_list = filter(lambda x: x not in english_stopwords, word_list)
    return set(important_word_list)
 
def get_top_topic(word_set):
    word_count_list = []
    for word in word_set:
	if word in word_count_dict:
            word_count_list.append(word_count_dict[word])
    #query = "word = " + "' and word = '".join(word_set) + "'"
    #word_count_list = freq_vector_df.select("counts").where(query).rdd.map(lambda row: map(int, row.counts.split())).collect()
    if not word_count_list:
        return 'No matched reddit'
    word_count_list = [sum(x) for x in zip(*word_count_list)]
    percentage_list = map(lambda x,y: x/float(y) if y != 0 else 0, word_count_list, total_count_list )
    return subreddit_list[ percentage_list.index(max(percentage_list)) ]
    #return random.sample(subreddit_list, 1)[0]

#parsed.map(lambda tweet: tweet['text']).pprint()
parsed.map(lambda tweet: get_word_set(tweet['text'])).pprint()
subreddit_topic = parsed.map(lambda tweet: get_top_topic(get_word_set(tweet['text'])))
subreddit_topic.pprint()
    
def update_tweet_count(partition):
    cluster = Cluster(['10.0.0.4'])
    session = cluster.connect('playground')

    query = "SELECT content FROM others WHERE category = 'tweet_count'"
    response = session.execute(query)
    tweet_count_dict = json.loads(response[0].content)

    for row in partition:
        if row in tweet_count_dict:
    	    tweet_count_dict[row] += 1

    query = "INSERT INTO others (category, content) VALUES ('tweet_count', '%s')" % (json.dumps(tweet_count_dict))
    session.execute(query)
    session.shutdown()
    return

subreddit_topic.foreachRDD( lambda rdd: rdd.foreachPartition(update_tweet_count) )
ssc.start()  
ssc.awaitTermination() 


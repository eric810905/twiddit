#    Spark
from pyspark import SparkContext  
#    Spark Streaming
from pyspark.streaming import StreamingContext  
#    Kafka
from pyspark.streaming.kafka import KafkaUtils  
#    json parsing
import json 
import string


sc = SparkContext(appName="spark_streaming_kafka")
sc.setLogLevel("WARN")

freq_vector_df = sc.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="freq_vector", keyspace="playground")\
    .load()

others_df = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="others", keyspace="playground")\
    .load()
    
subreddit_list = others_df.select("content").where("category = 'subreddits'").collect()
subreddit_list = subreddit_list[0][0].split()
total_count_list = others_df.select("content").where("category = 'total_counts'").collect()
total_count_list = map(int, total_count_list[0][0].split())

ssc = StreamingContext(sc, 10) 

kafkaStream = KafkaUtils.createStream(ssc, 'ec2-35-161-255-24.us-west-2.compute.amazonaws.com:2181', 'spark-streaming', {'twitter':1}) 

parsed = kafkaStream.map(lambda v: json.loads(v[1])) 

parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()

def get_word_set(tweet_text):
    tweet_text = filter ( lambda x: x in set(string.printable), tweet_text)
    return set(tweet_text.split())
    
def get_top_topic(word_set):
    query = "word = " + "' and word = '".join(word_set) + "'"
    word_count_list = freq_vector_df.select("counts").where(query).rdd.map(lambda row: map(int, row.counts.split())).collect()
    word_count_list = [sum(x) for x in zip(*word_count_list)]
    percentage_list = map(lambda x,y: x/float(y) if y != 0 else 1, word_count_list, total_count_list )
    return subreddit_list[ percentage_list.index(max(percentage_list)) ]

parsed.map(lambda tweet: tweet['text']).pprint()
subreddit_topic = parsed.map(lambda tweet: get_top_topic(get_word_set(tweet['text'])))
subreddit_topic.pprint()

#parsed.map(lambda x: type(x)).pprint()
#parsed.map(lambda x: filter ( lambda y: y in set(string.printable), x['text']) ).pprint()

#parsed.map(lambda x: x['lang'] ).pprint()
print("here!!!!!!!!!!!!!!!!")

#print(type(parsed))
#parsed.map(lambda tweet: tweet['text'])

#parsed.pprint()
#content = parsed.map(lambda tweet: tweet['text'])  
#content.pprint()
ssc.start()  
ssc.awaitTermination() 



#    Spark
from pyspark import SparkContext  
#    Spark Streaming
from pyspark.streaming import StreamingContext  
#    Kafka
from pyspark.streaming.kafka import KafkaUtils  
#    json parsing
import json 

sc = SparkContext(appName="spark_streaming_kafka")
sc.setLogLevel("WARN")

ssc = StreamingContext(sc, 10) 

kafkaStream = KafkaUtils.createStream(ssc, 'ec2-35-161-255-24.us-west-2.compute.amazonaws.com:2181', 'spark-streaming', {'twitter':1}) 

parsed = kafkaStream.map(lambda v: json.loads(v[1])) 

parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()

parsed.map(lambda x: type(x)).pprint()
parsed.map(lambda x: x['text'] ).pprint()
#parsed.map(lambda x: x['lang'] ).pprint()
print("here!!!!!!!!!!!!!!!!")

#print(type(parsed))
#parsed.map(lambda tweet: tweet['text'])

#parsed.pprint()
#content = parsed.map(lambda tweet: tweet['text'])  
#content.pprint()
ssc.start()  
ssc.awaitTermination() 



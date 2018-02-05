"""
This script batch process reddit dataset to generate the term frequency table
for every subreddits. The resulting table will be used to classify twitter 
message.

use the following command to run this file:
spark-submit --conf spark.cassandra.connection.host=10.0.0.xyz \
--packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.6 \
--executor-memory 6G --master spark://10.0.0.xyz:7077 \
reddit_batch_process.py
"""
from pyspark import SparkContext
from pyspark.sql import SQLContext
from nltk.corpus import stopwords
from cassandra.cluster import Cluster
import cassandra
from collections import Counter, defaultdict
import os.path
import json
import re

class redditBatchProcess( object ):
    """ Batch process the reddit data set and store the result for 
    classification in Cassandra database.
    """
    def __init__( self ):
        # configurations
	with open(os.path.dirname(__file__) + "/../config.json", "r") as f:
	    self.config = json.load(f)
	
        self.english_stopwords = stopwords.words("english")

        # total word count for every subreddit
	self.total_word_count_dict = {}

    def word_process(self, text):
        """ Transform a string into a set of important words in this string.
        Args:
    	  text: string
        Returns: set of important words
        """
	text_str = text.lower()
        word_list = re.findall(r'\w+', text_str, \
            flags = re.UNICODE | re.LOCALE)
	important_word_list = filter(lambda x: x not in \
            self.english_stopwords, word_list)
	return important_word_list

    def generate_word_count(self, row):
	""" Calculate the count of each word given a row of reddit comment.
        Returns: a list of tuples having the word and the count
	"""
	word_counter = Counter(row[1])
	return [ (word, [ (row[0], word_counter[word]) ] ) \
            for word in word_counter ]

    def construct_subreddit_word_freq(self, row):
	""" Generate the term freqency for each subreddit given a word.
        The counts are represnted by json string format.
        Returns: tuple of word and word counts in json string format
	"""
        # list of tuple (subreddit, count)
	word, subreddit_word_count_list = row

        # count the number of this word in the subreddit respectively
	subreddit_counter = Counter()
	for subreddit, word_count in subreddit_word_count_list:
            subreddit_counter[subreddit] += word_count
        
        # convert counter to dict
	subreddit_dict = dict( {k: v/float(self.total_word_count_dict[k]) \
            if self.total_word_count_dict[k] != 0 else 0 \
            for k, v in subreddit_counter.most_common()} )
	return (word, json.dumps(subreddit_dict))

    def start( self ):
        """ start batch processing
        """
	sc = SparkContext(appName="spark_batch_process")
	sc.setLogLevel("WARN")
	sqlContext = SQLContext(sc)

	# load reddit data from S3
	df = sqlContext.read.\
             json(self.config["REDDIT_BATCH_PROCESS"]["S3_DATA_PATH"])

	cluster = Cluster([self.config["DEFAULT"]["DBCLUSTER_PRIVATE_IP"]])
	session = cluster.connect(self.config["CASSANDRA"]["KEYSPACE"])

        # create a new table. truncate the table if it exists.
	try:
	    query = "CREATE TABLE %s (word text, counts text,\
                PRIMARY KEY (word), )" % \
                self.config["CASSANDRA"]["WORD_FREQUENCY_TABLE"]
	    response = session.execute(query)
	except cassandra.AlreadyExists:
	    query = "TRUNCATE %s" % \
                self.config["CASSANDRA"]["WORD_FREQUENCY_TABLE"]
	    response = session.execute(query)

	try:
	    query = "CREATE TABLE %s (subreddit text, word_count counter, \
                PRIMARY KEY (subreddit), )" % \
                self.config["CASSANDRA"]["WORD_COUNT_TABLE"]
	    response = session.execute(query)
	except cassandra.AlreadyExists:
	    query = "TRUNCATE %s" %\
                 self.config["CASSANDRA"]["WORD_COUNT_TABLE"]
	    response = session.execute(query)

	# aggregate the reddit comments by subreddit and counts the total word
        # count for each subreddit
	subreddit_word_rdd = df.rdd.\
            map(lambda row: (row.subreddit, self.word_process(row.body)))
	subreddit_word_count_df = \
            subreddit_word_rdd.map(lambda row: (row[0], len(row[1])))\
	    .reduceByKey( lambda a, b: a + b ).toDF(["subreddit", "word_count"])

	subreddit_word_count_df.write\
	    .format("org.apache.spark.sql.cassandra")\
	    .mode('append')\
	    .options(table=self.config["CASSANDRA"]["WORD_COUNT_TABLE"], \
            keyspace=self.config["CASSANDRA"]["KEYSPACE"])\
	    .save()

	# construct a dict of total word counts of every subreddits
	query = "SELECT * FROM %s" % \
            self.config["CASSANDRA"]["WORD_COUNT_TABLE"]
	response = session.execute(query)

	for row in response:
	    self.total_word_count_dict[row.subreddit] = row.word_count

	session.shutdown()

	# construct the word counts as json string format for each word and 
        # store the result to cassandra
	group_word_rdd = subreddit_word_rdd\
            .flatMap( self.generate_word_count )\
	    .reduceByKey(lambda a, b: a + b)

	group_subreddit_df = group_word_rdd\
            .map(self.construct_subreddit_word_freq).toDF(["word", "counts"])

	group_subreddit_df.write\
	    .format("org.apache.spark.sql.cassandra")\
	    .mode('append')\
	    .options(table=self.config["CASSANDRA"]["WORD_FREQUENCY_TABLE"], \
            keyspace=self.config["CASSANDRA"]["KEYSPACE"])\
	    .save()

	return

def main():
    process = redditBatchProcess()
    process.start()
    return

if __name__ == '__main__':
    main()

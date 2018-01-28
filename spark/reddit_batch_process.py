"""
This script batch process reddit data set to generate a table of word counts for each word in every subreddit
"""
from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import MapType,StringType,IntegerType
from collections import Counter
import re
from nltk.corpus import stopwords

sc = SparkContext(appName="spark_streaming_kafka")
sc.setLogLevel("WARN")
sqlContext = SQLContext(sc)

# load reddit data from S3
path = "s3n://erictsai/201701_top100000.json"
df = sqlContext.read.json(path)

english_stopwords = stopwords.words("english")

def word_process(rdd):
    text_str = rdd.all_text.lower()
    word_list = re.findall(r'\w+', text_str, flags = re.UNICODE | re.LOCALE)
    important_word_list = filter(lambda x: x not in english_stopwords, word_list)
    # this can be simplify
    return rdd.subreddit, " ".join(important_word_list)

# aggregate the reddit comments by subreddit and counts the total word count for each subreddit
group_subreddit_df = df.groupby("subreddit").agg(F.concat_ws(" ", F.collect_list('body')).alias('all_text'))
group_subreddit_df = group_subreddit_df.rdd.map(word_process).toDF(["subreddit", "all_text"])
total_word_count_df = group_subreddit_df.rdd.map(lambda row: (row.subreddit, row.all_text.count(" ")))

# a list of unique subreddits
subreddit_list = group_subreddit_df.select('subreddit').rdd.flatMap(lambda x: x).collect()
subreddit_list = subreddit_list[:1000]
print("number of reddits: %d" % len(subreddit_list))

# dictionary mapping the subreddit name to its index
indicesMap = dict(zip(subreddit_list, range(len(subreddit_list))))

# calculate the count of each word for every subreddit
#udf1 = F.udf(lambda x: dict(Counter(x.split())),MapType(StringType(),IntegerType()))
#word_count_df = df.groupby("subreddit").agg(udf1(F.concat_ws(" ", F.collect_list('body'))).alias('word_frequency'))
word_count_df = group_subreddit_df.rdd.map(lambda rdd: (rdd.subreddit, dict(Counter(rdd.all_text.split())))).toDF(['subreddit', 'word_frequency'])

# exchange the key of rdd from subreddit to word
word_to_subreddit = word_count_df.rdd.flatMap(lambda row:  [( word, [(row.subreddit, (row.word_frequency[word]))] ) for word in row.word_frequency]) \
                 .reduceByKey(lambda a, b: a + b)

# generate the word counts for each subreddit. the counts are represnted by string
def construct_count_list(row):
    word, counts = row[0], row[1]
    count_list = [ 0 for _ in range(len(subreddit_list))]
    for (subreddit, count) in counts:
        if subreddit in indicesMap:
            count_list[ indicesMap[subreddit] ] = count
    count_string = " ".join(map(str, count_list))
    return (word, count_string)

# construct the string of word counts for each word and store the result to cassandra
count_string_df = word_to_subreddit.map(construct_count_list)
count_string_df = count_string_df.toDF(["word", "counts"])

count_string_df.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="freq_vector", keyspace="playground")\
    .save()

# a list of total word counts of the subreddits
total_word_count_list = [ 0 for _ in range(len(subreddit_list))]
for subreddit, total_count in total_word_count_df.collect():
    if subreddit in indicesMap:
        total_word_count_list[indicesMap[subreddit]] = total_count

spark = SparkSession(sc).builder\
                .master("spark://10.0.0.5:7077")\
                .appName("spark_stream")\
                .config("--packages", "com.datastax.spark:spark-cassandra-connector_2.11:2.0.6")\
                .getOrCreate()

# store the subreddit names and total word counts
spark.createDataFrame([{'category':'subreddits', 'content': " ".join(subreddit_list)}, {'category':'total_counts', 'content': " ".join(map(str, total_word_count_list))}]).write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="others", keyspace="playground")\
    .save()


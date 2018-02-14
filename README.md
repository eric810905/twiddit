# Twiddit
Use reddit comments dataset to classify the twitter messages to the subreddit topics. The tweet is classified to a subreddit topic with the highest term frequency–inverse document frequency (tf-idf) metric.

# Demo
http://twiddit.site

or

http://ec2-34-213-107-177.us-west-2.compute.amazonaws.com/demo

# Purpose and Use Cases
Knowing what people are talking about and the people's interests are useful for digital marketing and social media monitoring.

# Proposed Architecture
See the slides for the data pipeline:
http://twiddit.site/slides

or

https://docs.google.com/presentation/d/1-tYs0eIeKXNV5WvOdctHIi5myJ9o4_DX8Mx5SVk2wug/edit#slide=id.g306586f75a_0_725

# Technologies well-suited to solve the challenges
Spark - batch process reddit dataset to generate term-frequency table for every word and every subreddit.

Spark Streaming - stream process the twitter message and classify.

Kafka - handle the twitter streaming messages.

Cassandra - a database with high availability for Spark Streaming to query the term-frequency table and class.

# The primary engineering challenges
Streaming process tiwtter message at the rate of 1000/sec. Classify every tweet into over 34,000 subreddit topics.

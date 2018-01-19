# Project Idea
Use huge dynamic knowledge base (reddit) to identify the topic of the contenct on social media in real-time.

# Purpose and the most common use cases
Knowing what people are talking about is useful for digital marketing to understand the users' interests. 

# Technologies well-suited to solve the challenges
Elasticsearch - extract the statistics of the knowledge base.
Kafka - Need the data ingestion system to analyze the real-time social media data.    
Spark streaming - stream process the real-time social media data.

# Proposed architecture
Elasticsearch (Reddit Dataset) 
                              \ 
                                -> Spark Steaming -> Flask
                              /
  (Twitter) ->        Kafka 

# The primary engineering challenges?
Load and construct the index of 2TB Reddit dataset into Elasticsearch.
Streaming process real-time tiwtter data.

# The (quantitative) specifications/constraints.
Process the twitter data within a minute.
Update Reedit Knowledge base every minute.

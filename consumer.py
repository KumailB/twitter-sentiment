import os
from textblob import TextBlob
from elasticsearch import Elasticsearch
from kafka import KafkaConsumer
#from dotenv import load_dotenv
import json

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

#load_dotenv()

es_cloud_id = "6350-hw4:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvJDczYzBkMzI1ZGE2YjQxMDU4NjA5Mjg5ZWFjNjMyNDdlJGUzZDJmY2ViNzEzOTQ5MGU5OTU4MDRmMGQwM2Q3Y2Q3" #os.getenv('ES_CLOUD_ID')
es_username = "elastic"#os.getenv('ES_USERNAME')
es_password = "YL8gTusa3ATuVXhpiHppdWCL"#os.getenv('ES_PASSWORD')

es = Elasticsearch(cloud_id=es_cloud_id, http_auth=(es_username, es_password))
conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
sc = SparkContext(conf=conf)

# Creating a streaming context with batch interval of 10 sec
ssc = StreamingContext(sc, 10)
ssc.checkpoint("checkpoint")
kstream = KafkaUtils.createDirectStream(
ssc, topics = ['quickstart-events'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
tweets = kstream.collect(1)
print(tweets)

  # dict_data = json.loads(msg.value)
  # tweet = TextBlob(dict_data["text"])
  # polarity = tweet.sentiment.polarity
  # tweet_sentiment = ""
  # if polarity > 0:
  #     tweet_sentiment = 'positive'
  # elif polarity < 0:
  #     tweet_sentiment = 'negative'
  # elif polarity == 0:
  #     tweet_sentiment = 'neutral'

  # # add text & sentiment to es
  # es.index(
  #     index="tweet_es_" + hashtag + "_index",
  #     doc_type="test_doc",
  #     body={
  #         "author": dict_data["user"]["screen_name"],
  #         "date": dict_data["created_at"],
  #         "message": dict_data["text"],
  #         "sentiment": tweet_sentiment
  #     }
  # )
  # print(str(tweet))
  # print('\n')

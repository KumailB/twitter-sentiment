# import os
# from textblob import TextBlob
# from elasticsearch import Elasticsearch
from kafka import KafkaConsumer
# from dotenv import load_dotenv
# import json

from pyspark.sql import SparkSession



# es_cloud_id = os.getenv('ES_CLOUD_ID')
# es_username = os.getenv('ES_USERNAME')
# es_password = os.getenv('ES_PASSWORD')

spark = SparkSession \
    .builder \
    .appName("TweetTester") \
    .getOrCreate()


df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "tweets") \
  .option("startingOffsets", "earliest") \
  .load()

from pyspark.sql.types import StringType

# Convert binary to string key and value
df1 = (df
    .withColumn("key", df["key"].cast(StringType()))
    .withColumn("value", df["value"].cast(StringType())))

df1.show(5)
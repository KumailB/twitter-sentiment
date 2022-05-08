import os
import tweepy
from kafka import KafkaProducer
from dotenv import load_dotenv
import json

load_dotenv()

token = os.getenv('TWITTER_BEARER_TOKEN')

class Producer(tweepy.StreamingClient):
    def __init__(self, token):
        super(Producer, self).__init__(token)
        # localhost:9092 = Default Zookeeper Producer Host and Port Adresses
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    # Get Producer that has topic name is Twitter
    # self.producer = self.client.topics[bytes("twitter")].get_producer()

    def on_data(self, data):
        # Producer produces data for consumer
        # Data comes from Twitter
        print(data)
        callback = self.producer.send("tweets", ("twitter_stream_" + hashtag).encode('utf-8'))
        try:
            record_metadata = callback.get(timeout=10)
        except KafkaError:
        # Decide what to do if produce request failed...
            print("Failed!")
            log.exception()
            pass
        print("Sent")
        return True


streaming_client = Producer(token)

hashtag = input("Enter the hashtag : ")
hashStr = "#" + hashtag

# Produce Data that has trump hashtag (Tweets)
streaming_client.add_rules(tweepy.StreamRule(hashStr))

streaming_client.filter()

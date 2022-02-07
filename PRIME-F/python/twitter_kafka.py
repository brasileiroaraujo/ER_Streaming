#!/usr/bin/python3

# Requires the installation of Tweepy library

import time
import json
from kafka import KafkaConsumer, KafkaProducer
from getpass import getpass
from textwrap import TextWrapper
from pymongo import MongoClient
import datetime as dt
import tweepy
import twitterapi.trending_topics_twitter as trending_topics
# from symbol import except_clause
from twitterapi.twitter_object import twitter_object
# from _ast import If


class StreamWatcherListener(tweepy.StreamListener):

    status_wrapper = TextWrapper(width=30, initial_indent='    ', subsequent_indent='    ')
    eventDB = None
    gtDB = None
    stop_condition = ""
    topics=[]
    
    def __init__(self, api, search_words):
        self.api = api
        self.me = api.me()
        self.topics = [topic.replace("#", "").replace(" ", "").lower() for topic in search_words]

    
    def publish_message(self, producer_instance, topic_name, key, value):
        try:
            key_bytes = bytes(key, encoding='utf-8')
            value_bytes = bytes(value, encoding='utf-8')
            producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
            producer_instance.flush()
            print('Message published successfully.')
        except Exception as ex:
            print('Exception in publishing message')
            print(str(ex))


    def connect_kafka_producer(self):
        _producer = None
        try:
            _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
        except Exception as ex:
            print('Exception while connecting Kafka')
            print(str(ex))
        finally:
            return _producer
    
    def connect_mongoDB_Twitter(self):
        if self.eventDB == None:
            client = MongoClient('mongodb://localhost:27017/EventsProject')
            return client.EventProject18.Twitter
        else:
            return self.eventDB
    
    def connect_mongoDB_Groudtruth(self):
        if self.gtDB == None:
            client = MongoClient('mongodb://localhost:27017/EventsProject')
            return client.EventProject18.Groundtruth
        else:
            return self.gtDB
        
    def on_status(self, status):
        try:
            #Stop condition
            if (dt.datetime.now().strftime('%H:%M') == self.stop_condition):
                return False
            
            #print(self.status_wrapper.fill(status.text))
            #print('\n %s  %s  via %s\n' % (status.author.screen_name, status.created_at, status.source))
            
            json_tw = status._json
            
            #saving on the database (mongo)
            self.eventDB = self.connect_mongoDB_Twitter()
            result = self.eventDB.insert_one(json_tw.copy())
            # print(result)
            
            
            #print(eventDB.command("serverStatus"))
            #post = {"author": "Mike","text": "My first blog post!","tags": ["mongodb", "python", "pymongo"]}
            #print(result.inserted_id)
            
            text = " "
            if 'text' in json_tw:
                text = json_tw['text']
            full_text = " "
            if 'extended_tweet' in json_tw:
                extended_tweet = json_tw['extended_tweet']
                if 'full_text' in extended_tweet:
                    full_text = extended_tweet['full_text']
            
            user_name = json_tw['user']['name']
            user_screen_name = json_tw['user']['screen_name']
            user_description = json_tw['user']['description']
            
            quoted_status_text = " "
            if 'quoted_status' in json_tw:
                quoted_status = json_tw['quoted_status']['text']
            
            hashtags = ""
            if 'entities' in json_tw and 'hashtags' in json_tw['entities']:
                for tag in json_tw['entities']['hashtags']:
                    hashtags += tag['text'].lower() + " "
                
            user_cited = json_tw['entities']['user_mentions']
                
            twitter_obj = twitter_object(json_tw['id_str'],json_tw['created_at'], text, full_text, user_name, user_screen_name, user_description, hashtags.strip(), user_cited)
            #print(twitter_obj.formatToStandardString())
              
            #publishing on kafka
            kafka_producer = self.connect_kafka_producer()
            self.publish_message(kafka_producer, 'mytopicTwitter', 'twitter', twitter_obj.formatToStandardString())
            
            
            
            #getting the ground truth
            if hashtags != "":
                all_hashtags = hashtags.split(" ")
                intersection = [value for value in all_hashtags if value in self.topics]
                self.gtDB = self.connect_mongoDB_Groudtruth()
                for tag in intersection:
                    json_object = json.loads(json.dumps({'id_tweet':json_tw['id_str'], 'hashtag':tag}))
                    self.gtDB.insert_one(json_object)
            
            
            print(twitter_obj.formatToStandardString())
            print('----------------------------------')
            
            # return self
        except Exception as e:
            print('Failed to upload to ftp: '+ str(e))
            #print('Exception while sending twitters to kafka.')
            # Catch any unicode errors while printing to console
            # and just ignore them to avoid breaking application.
            pass

    def on_error(self, status_code):
        print('An error has occured! Status code = %s' % status_code)
        return False  # keep stream alive

    def on_timeout(self):
        print('Snoozing Zzzzzz')




def main(stop_hour):
    
    # Prompt for login credentials and setup stream object

    auth = tweepy.auth.OAuthHandler(consumer_key, consumer_secret)
    
    search_words = trending_topics.getTrendingTopicsStandard()
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    
    auth.set_access_token(access_token, access_token_secret)
    
    listener = StreamWatcherListener(api = api, search_words = search_words)
    # listener.stop_condition = stop_hour
    
    stream = tweepy.Stream(auth, listener, timeout=None)
    #https://boundingbox.klokantech.com/ site to get the cities bounding box
    brazil_bounding_box = [-73.712054491,-33.5974337638,-34.337054491, 6.2921284478] #recife -34.977052,-8.155187,-34.82393,-7.949604 #[-49.381,-25.572,-49.118,-25.318] #brazil:-73.712054491,-33.5974337638,-34.337054491,6.2921284478
    usa_bounding_box = [-126.79,24.25,-63.0,50.18]
    
    
    #, track=search_words
    stream.filter(locations=usa_bounding_box, languages=["en"], track=search_words) #en = english, es = spanish, pt = portuguese
    
    stream.disconnect()
    
    #return self

if __name__ == '__main__':
    try:
        main('23:35')
    except KeyboardInterrupt:
        print('\nGoodbye!')
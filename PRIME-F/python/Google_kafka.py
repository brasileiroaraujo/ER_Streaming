#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Requires the installation of Tweepy library

import feedparser
from kafka import KafkaProducer
from pymongo import MongoClient
from GoogleNewApi.Google_object import Google_object
import time;
import twitterapi.trending_topics_twitter as trending_topics_api
from datetime import datetime

trending_topics = None


# from symbol import except_clause
# from _ast import If
def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print(value)
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer
    
def connect_mongoDB_Twitter():
    client = MongoClient('mongodb://localhost:27017/EventsProject')
    return client.EventProject18.Google

def isPublishedToday(new):
    date_pub = datetime.strptime(new["published"], '%a, %d %b %Y %H:%M:%S %Z')
    return date_pub.date() == datetime.today().date()

def getNewsFromTrendingTopics(all_news_old):
    google_news = []
    global trending_topics
    if trending_topics == None:
        trending_topics = trending_topics_api.getTrendingTopicsSorted()
    for topic in trending_topics:
        news = feedparser.parse("https://news.google.com/rss/search?q=" + topic + "&hl=en-US&gl=US&ceid=US%3Aen").entries
        for new in news[0:10]:#top-10 relevant news
            if isPublishedToday(new) and new.id not in all_news_old:
                new['hashtag'] = topic
                google_news.append(new)
    return google_news

try:
    kafka_producer = connect_kafka_producer()
    eventDB = connect_mongoDB_Twitter()
    # health_news_old = []
    # politic_news_old = []
    all_news_old = []
    
    while True:
        #all_news = feedparser.parse("https://news.google.com/rss/topics/CAAqKggKIiRDQkFTRlFvSUwyMHZNREpxYW5RU0JYQjBMVUpTR2dKQ1VpZ0FQAQ?hl=pt-BR&gl=BR&ceid=BR%3Apt-419").entries
        all_news = getNewsFromTrendingTopics(all_news_old)
        all_news =  [item for item in all_news if item.id not in all_news_old] #difference between sets, removing equal news
        
        #setting the historical data
        all_news_old += [item.id for item in all_news]
        
        for new in all_news:
            Google_obj = Google_object(new.copy())
            publish_message(kafka_producer, 'mytopicNews', 'gl', Google_obj.formatToStandardString())
            
            #store on mongodb
            eventDB.insert_one(new.copy())#avoid to modify the hashcode of the object and break the old news control
            
            print(Google_obj.toString())
            print("-----------------------------------------------------------------------------------------")
        print(len(all_news))
        time.sleep(60)
except Exception as ex:
    print(str(ex))
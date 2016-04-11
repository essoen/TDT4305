#!/usr/bin/env python

# author: Stein-Otto Svorstoel
# email: steinotto@svorstol.com

# Task
# Find aggregated polarity (sentiment) of all English tweets (lang
# = `en') for each city in the United States (place type = `city',
# country = `US') for each day of week.

# Solution
# 1. Find all english tweets
# 2. Seperate them by city in the US
# 3. Seperate them by day of week
# 4. Aggregate polarity and log to file for each day of the week for each city


import sys
import datetime
import calendar

from pyspark import SparkContext

INPUT_DATA_PATH = sys.argv[1]
OUTPUT_DATA_FILE = sys.argv[2]

sc = SparkContext("local[*]", "Twitter Analysis")

twitterData = sc.textFile(INPUT_DATA_PATH + '/geotweets.tsv', use_unicode=False)


def createTwitterDict(t):
    tweet = t.split('\t') # split on tab
    return {
        "utc_time": tweet[0],
        "country_code": tweet[2],
        "place_type": tweet[3],
        "city_name": tweet[4],
        "language": tweet[5],
        "user_timezone_offset": tweet[8],
        "tweet": tweet[10],
    }

positiveWords = open(INPUT_DATA_PATH + "positive-words.txt", "r").read().splitlines()
negativeWords = open(INPUT_DATA_PATH + "negative-words.txt", "r").read().splitlines()
def calculateTextSentiment(text):
    sent = 0
    for word in text.split():
        if(word in positiveWords): sent +=1
        elif(word in negativeWords): sent -=1
    return sent

def getWeekday(timestamp):
    correctedTimestmap = float(str(timestamp)[:10] + "." + str(timestamp)[10:13]) #for some reason the timestamp has length 12
    return calendar.day_name[datetime.datetime.utcfromtimestamp(correctedTimestmap).weekday()]

def add(a,b):
    return a+b

tweets = twitterData \
    .map(createTwitterDict).persist()\
    .filter(lambda tweet: tweet["language"] == "en"
                          and tweet["country_code"] == "US"
                          and tweet["place_type"] == "city") \
    .map(lambda tweet: ((tweet["city_name"], getWeekday(tweet["utc_time"])), calculateTextSentiment(tweet["tweet"]))) \
    .combineByKey(int, add, add  ) \
    .map(lambda K, V: K[0] + "\t" + K[1] + str(V) ) \
    .saveAsTextFile(OUTPUT_DATA_FILE)

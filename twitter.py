#!/usr/bin/env python

# author: Stein-Otto Svorstoel
# email: steinotto@svorstol.com

# Task
# Find aggregated polarity (sentiment) of all English tweets (lang
# = `en') for each city in the United States (place type = `city',
# country = `US') for each day of week.


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

positiveWords = sc.textFile("hdfs://dascosa09.idi.ntnu.no:8020/user/janryb/positive-words.txt").collect()
negativeWords = sc.textFile("hdfs://dascosa09.idi.ntnu.no:8020/user/janryb/negative-words.txt").collect()
def calculateTextSentiment(text):
    sent = 0
    words = text.split()
    for word in words:
        if(word in positiveWords): sent +=1
        elif(word in negativeWords): sent -=1
    return sent

def getWeekday(timestamp):
    correctedTimestmap = float(str(timestamp)[:10] + "." + str(timestamp)[10:13]) #for some reason the timestamp has length 12
    return calendar.day_name[datetime.datetime.utcfromtimestamp(correctedTimestmap).weekday()]

def add(a,b):
    return a+b

tweets = twitterData \
    .filter(lambda tweet: tweet[5] == "en"
                          and tweet[2] == "US"
                          and tweet[3] == "city") \
    .map(lambda tweet: ((tweet[4], getWeekday(tweet[0])), calculateTextSentiment(tweet[10]))) \
    .combineByKey(int, add, add  ) \
    .map(lambda row: row[0][0] + "\t" + row[0][1] +  "\t" + str(row[1]) ) \
    .saveAsTextFile(OUTPUT_DATA_FILE)

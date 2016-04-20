#!/usr/bin/env python

# author: Stein-Otto Svorstoel
# email: steinotto@svorstol.com

import sys
import datetime
import calendar
from datetime import timedelta

from pyspark import SparkContext

INPUT_DATA_PATH = sys.argv[1]
OUTPUT_DATA_FILE = sys.argv[2]

sc = SparkContext("local[*]", "Twitter Analysis")

twitterData = sc.textFile(INPUT_DATA_PATH + '/geotweets.tsv', use_unicode=False)

positive_words = sc.textFile("hdfs://dascosa09.idi.ntnu.no:8020/user/janryb/positive-words.txt").collect()
negative_words = sc.textFile("hdfs://dascosa09.idi.ntnu.no:8020/user/janryb/negative-words.txt").collect()
def calculate_text_sentiment(text):
    sent = 0
    words = text.split()
    for word in words:
        if word.lower() in positive_words: sent +=1
        elif word.lower() in negative_words: sent -=1
    return sent

def get_weekday(timestamp, offset):
    corrected_timestamp = float(str(timestamp)[:10] + "." + str(timestamp)[10:13]) #for some reason the timestamp has length 12
    timezone_offset = timedelta(minutes=int(offset))
    local_time = datetime.datetime.utcfromtimestamp(corrected_timestamp) + timezone_offset
    print(local_time)
    return calendar.day_name[local_time.weekday()]

def add(a,b):
    return a+b

tweets = twitterData \
    .filter(lambda tweet: tweet[5] == "en"
                          and tweet[2] == "US"
                          and tweet[3] == "city") \
    .map(lambda tweet: ((tweet[4], get_weekday(tweet[0], tweet[8])), calculate_text_sentiment(tweet[10]))) \
    .combineByKey(int, add, add) \
    .map(lambda row: row[0][0] + "\t" + row[0][1] +  "\t" + str(row[1]) ) \
    .saveAsTextFile(OUTPUT_DATA_FILE)

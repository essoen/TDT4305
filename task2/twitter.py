#!/usr/bin/env python

# author: Stein-Otto Svorstoel
# email: steinotto@svorstol.com

import sys
import datetime
import calendar
from datetime import timedelta

from pyspark import SparkContext, SparkConf

INPUT_DATA_PATH = sys.argv[1]
OUTPUT_DATA_FILE = sys.argv[2]

conf = (SparkConf().setAppName("TDT4305 Task 2: Twitter Analysis"))
sc = SparkContext(conf=conf)

twitter_dataset = sc.textFile(INPUT_DATA_PATH + '/geotweets.tsv', use_unicode=False)

positive_words = sc.textFile("hdfs://dascosa09.idi.ntnu.no:8020/user/janryb/positive-words.txt").collect()
negative_words = sc.textFile("hdfs://dascosa09.idi.ntnu.no:8020/user/janryb/negative-words.txt").collect()

LOOKUP_TABLE = {}
for word in positive_words: LOOKUP_TABLE[word] = 1
for word in negative_words: LOOKUP_TABLE[word] = -1

def calculate_text_sentiment(text):
    sentiment = 0
    words = text.split()
    for word in words:
        try:
            sentiment += LOOKUP_TABLE[word.lower()]
        except KeyError:
            continue
    return sentiment

def get_weekday(timestamp, offset):
    corrected_timestamp = float(str(timestamp)[:10] + "." + str(timestamp)[10:13]) #for some reason the timestamp has length 12
    timezone_offset = timedelta(minutes=int(offset))
    local_time = datetime.datetime.utcfromtimestamp(corrected_timestamp) + timezone_offset
    return calendar.day_name[local_time.weekday()]

def add(a,b):
    return a +  b

def pretty_print_row(row):
    return row[0][0] + "\t" + row[0][1] +  "\t" + str(row[1])

twitter_dataset \
    .map(lambda row: row.split("\t")) \
    .filter(lambda tweet: tweet[5] == "en"
                          and tweet[2] == "US"
                          and tweet[3] == "city") \
    .map(lambda tweet: ((tweet[4], get_weekday(tweet[0], tweet[8])), tweet[10])) \
    .map(lambda row: (row[0], calculate_text_sentiment(row[1]))) \
    .reduceByKey(add) \
    .map(pretty_print_row) \
    .coalesce(1) \
    .saveAsTextFile(OUTPUT_DATA_FILE)
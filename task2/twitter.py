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

sc = SparkContext("local[*]", "TDT4305 Task 2: Twitter Analysis")

twitter_dataset = sc.textFile(INPUT_DATA_PATH + '/geotweets.tsv', use_unicode=False)

positive_words = sc.textFile("hdfs://dascosa09.idi.ntnu.no:8020/user/janryb/positive-words.txt").collect()
negative_words = sc.textFile("hdfs://dascosa09.idi.ntnu.no:8020/user/janryb/negative-words.txt").collect()

def calculate_text_sentiment(text):
    sentiment = 0
    words = text.split()
    for word in words:
        lower_word = word.lower()
        if lower_word in positive_words: sentiment +=1
        elif lower_word in negative_words: sentiment -=1
    return sentiment

def get_weekday(timestamp, offset):
    corrected_timestamp = float(str(timestamp)[:10] + "." + str(timestamp)[10:13]) #for some reason the timestamp has length 12
    timezone_offset = timedelta(minutes=int(offset))
    local_time = datetime.datetime.utcfromtimestamp(corrected_timestamp) + timezone_offset
    return calendar.day_name[local_time.weekday()]

def add(a,b):
    return a+b

twitter_dataset \
    .map(lambda row: row.split("\t")) \
    .filter(lambda tweet: tweet[5] == "en"
                          and tweet[2] == "US"
                          and tweet[3] == "city") \
    .map(lambda tweet: ((tweet[4], get_weekday(tweet[0], tweet[8])), calculate_text_sentiment(tweet[10]))) \
    .combineByKey(int, add, add) \
    .map(lambda row: row[0][0] + "\t" + row[0][1] +  "\t" + str(row[1]) ) \
    .saveAsTextFile(OUTPUT_DATA_FILE)


# author: Andreas Drivenes
# email: andreas.drivenes@gmail.com


from pyspark import SparkContext, SparkConf
import sys
from mappers import (record_to_object, calculate_local_time, calculate_distance,
    find_nearest_city_and_country)
from utilities import haversine

conf = (SparkConf()
        .setMaster("local[*]")
        .setAppName("Foursquare Analysis"))

sc = SparkContext(conf=conf)

INPUT_DATA_PATH = sys.argv[1]

# 1. Load the Foursquare dataset.
fsData = sc.textFile(INPUT_DATA_PATH + '/dataset_TIST2015.tsv', use_unicode=False)
fsCountries = (sc.textFile(INPUT_DATA_PATH + '/dataset_TIST2015_Cities.txt', use_unicode=False)
    .map(lambda x: x.split('\t'))
    .collect())


#2. Calculate local time for each check-in (UTC time + timezone offset).
#3. Assign a city and country to each check-in (use Haversine formula described below).
checkins = (fsData
            .mapPartitionsWithIndex(lambda i, it: iter(list(it)[1:]) if i == 0 else it)
            .map(record_to_object)
            .map(calculate_local_time)
            .map(lambda x: find_nearest_city_and_country(x, fsCountries)))
checkins.persist() # checkins will be used a lot later


'''
4. Answer the following questions:
(a) How many unique users are represented in the dataset?
(b) How many times did they check-in in total?
(c) How many check-in sessions are there in the dataset?
(d) How many countries are represented in the dataset?
(e) How many cities are represented in the dataset?
'''

fields = {'user_id': 0, 'session_id': 0, 'country': 0, 'city': 0}

for field in fields.keys(): 
    unique_count = (checkins 
                        .map(lambda o: o[field]) 
                        .distinct() 
                        .count())
    fields[field] = unique_count

num_checkins = checkins.count()
print "Number of total checkins: %i" % num_checkins
for field in fields:
    print "Number of distinct %s : %i" % (field, fields[field])


'''
5. Calculate lengths of sessions as number of check-ins and provide a histogram
of these lengths.
'''

session_lengths = (checkins
    .map(lambda x: (x['session_id'], 1))
    .reduceByKey(lambda a, b: a + b) # number of checkins per session
    .map(lambda x: (str(x[1]), 1)) # use this number as a key for the histogram count
    .reduceByKey(lambda a, b: a + b)
    .saveAsTextFile('session_lengths'))


'''
6. For sessions with 4 and more check-ins, calculate their distance in kilometers
(use Haversine formula to compute distance between two pairs of geo.
coordinates).

7. Find 100 longest sessions (in terms of check-in counts) that cover distance
of at least 50Km.
'''

long_sessions = (checkins 
    .map(lambda o: ((o['session_id'], o)))
    .groupByKey()
    .mapValues(lambda o: list(o))
    .filter(lambda o: len(o[1]) >= 4) # session length bigger than 4
    .map(calculate_distance) # calculate total session distance
    .filter(lambda o: o[2] > 50.0) # only sessions with distance > 50 km
    .takeOrdered(100, key=lambda o: -len(o[1]))) # take the 100 longest sessions

'''
(a) For these 100 sessions, output data about their check-ins into a CSV
or TSV file. Use all available data fields such as checkin_id, session_id,
etc. and also add check-in date in `YYYY-MM-DD HH:MM:SS' format
'''

with open('results.tsv', 'w') as q:
    for session in long_sessions:
        (_, checkins, dist) = session
        for checkin in checkins:
            q.write('\t'.join(checkin.values() + [str(dist)]) + '\n')


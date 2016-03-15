from pyspark import SparkContext
import os

path = os.getcwd() 

sc = SparkContext("local", "Foursquare Analysis")
fsData = sc.textFile(path + '/dataset_TIST2015.tsv')

users = fsData.sample(False, 0.0001).map(lambda r: r.split('\t')[1])
users.persist()
unique_users = users.distinct()

print "Number of unique users: %i" % unique_users.count()
print "User: %s" % users.take(3)[0]
print "Number of check-ins: %i" % users.count()


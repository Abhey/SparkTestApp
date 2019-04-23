"""SparkApp.py"""
from pyspark import SparkContext
import os

dir = os.path.dirname(os.path.realpath(__file__))

dataFile = dir + "/data.txt"
sparkContext = SparkContext("local", "SparkApp")
data = sparkContext.textFile(dataFile).cache()

numAs = data.filter(lambda s: 'a' in s).count()
numBs = data.filter(lambda s: 'b' in s).count()

f = open(dir + "/output.txt", "a")
f.write("Lines with a: {0}, lines with b: {1}".format(numAs, numBs))
f.close()

"""SparkApp.py"""
from pyspark import SparkContext

dataFile = "./data.txt"
sparkContext = SparkContext("local", "SparkApp")
data = sparkContext.textFile(dataFile).cache()

numAs = data.filter(lambda s: 'a' in s).count()
numBs = data.filter(lambda s: 'b' in s).count()

f = open("output.txt", "a")
f.write("Lines with a: {0}, lines with b: {1}".format(numAs, numBs))
f.close()

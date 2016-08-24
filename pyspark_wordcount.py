# coding:utf-8 
import os

os.environ['SPARK_HOME'] = r'E:\spark-2.0.0-bin-hadoop2.7'

from pyspark import SparkConf, SparkContext

appName = "myApp"  
master = "local"
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

textFile=sc.textFile('word.txt')
wordCounts = textFile.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
textFile.count()
def p(x):
    print x
wordCounts.foreach(p)
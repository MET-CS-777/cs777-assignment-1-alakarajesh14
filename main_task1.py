from __future__ import print_function

import os
import sys
import requests
from operator import add

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *


#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False

#Function - Cleaning
#For example, remove lines if they don’t have 16 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0.1 miles, 
# fare amount and total amount are more than 0.1 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0):
                return p

#Main
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: main_task1 <file> <output> ", file=sys.stderr)
        exit(-1)
    
    sc = SparkContext(appName="Assignment-1")
    
    rdd = sc.textFile(sys.argv[1])

#Task 1
    cleaned_rdd = rdd.map(lambda line: line.split(",")) \
                        .filter(lambda p: correctRows(p) is not None)
    
    medallion_driver = cleaned_rdd.map(lambda p: (p[0], p[1]))
    unique_pairs = medallion_driver.distinct()
    
    driver_count = unique_pairs.map(lambda x: (x[0], 1)) \
                               .reduceByKey(add)
    
    results_1 = driver_count.sortBy(lambda x: x[1], ascending=False).zipWithIndex() \
                               .filter(lambda x: x[1] < 10) \
                               .map(lambda x: x[0])

    results_1.coalesce(1).saveAsTextFile(sys.argv[2])
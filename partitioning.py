#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 24 15:25:28 2022

@author: shambhavirege
"""
#!/usr/bin/env python
# -*- coding: utf-8 -*-

#Use getpass to obtain user netID
import getpass
import math
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

from pyspark.sql import Row
from pyspark.sql import SparkSession

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession


def main(spark):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    netID : string, netID of student to find files in HDFS
    '''


    # Load the boats.txt and sailors.json data into DataFrame
    movie_ratings = spark.read.csv('hdfs:/user/sa6523/ratings.csv',header=True, schema='userId INT, movieId INT, ratings FLOAT, timestamp INT')
    movie_ratings.show()
    movie_ratings.groupBy("userID").count().show()
    train=movie_ratings.select("userId").distinct().sampleBy("userID", fractions={0: 0.7}, seed=10)
    train.groupBy("userID").count().show()
   


# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get user netID from the command line
    netID = getpass.getuser()

    # Call our main routine
    main(spark)
    
    

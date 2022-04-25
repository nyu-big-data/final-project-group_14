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
from pyspark.sql import Row
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

    movie_ratings = spark.read.csv('hdfs:/user/sa6523/ratings.csv', header = True ,schema = 'userId STRING, movieId STRING, rating STRING, timestamp STRING')
    movie_ratings.show()
    
    movie_ratings.groupBy("userId").count().show()
    train=movie_ratings.sampleBy("userId", fractions={1: 0.6, 2: 0.6, 3: 0.6}, seed=10)
    
    train.groupBy("userId").count().show()
    train.show()
    
    test=movie_ratings.subtract(train)
    test.groupBy("userId").count().show()
    
    test.orderBy('userId').show()
    
    #window = Window.partitionBy('userId').orderBy('')
    test = test.select('userId','movieId','rating','timestamp', F.row_number().over(window).alias("row_number"))
    
    test_split = test.filter(test.userId % 2 == 1)
    val_split = test.filter(test.userId % 2 == 0)
    
    test_split.show()
    val_split.show()

    


# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get user netID from the command line
    netID = getpass.getuser()

    # Call our main routine
    main(spark)
    
    

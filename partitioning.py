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
    movie_ratings = spark.read.csv('hdfs:/user/sr6172/movielens/ml-lastest-small/ratings.csv',schema = 'userId STRING, movieId STRING, rating STRING, timestamp STRING')
    movie_ratings.createOrReplaceTempView('movie_ratings')
    res_1 = spark.sql('SELECT * from movie_ratings')
    res_1.show()
    
   


# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get user netID from the command line
    netID = getpass.getuser()

    # Call our main routine
    main(spark)
    
    

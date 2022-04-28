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
#import math
#from pyspark.sql import functions as F
#from pyspark.sql.window import Window
#from pyspark.sql import Row
# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession


def main(spark):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    netID : string, netID of student to find files in HDFS
    '''


    

    movie_ratings = spark.read.csv('hdfs:/user/sr6172/ratings.csv', header = True ,schema = 'userId INT, movieId INT, rating FLOAT, timestamp DOUBLE')
    movie_ratings.show()
    
    movie_ratings.createOrReplaceTempView('movie_ratings')
    
    partition = movie_ratings.select('userId').distinct().randomSplit([0.4, 0.3, 0.3], seed = 1234)
    
    train_users = tuple(list(x.userId for x in partition[0].collect()))
    val_users = tuple(list(x.userId for x in partition[1].collect()))
    test_users = tuple(list(x.userId for x in partition[2].collect()))
    
    train = spark.sql("SELECT * FROM movie_ratings WHERE userId in "+ str(train_users))
    test = spark.sql("SELECT * FROM movie_ratings WHERE userId in "+ str(test_users))
    val = spark.sql("SELECT * FROM movie_ratings WHERE userId in "+ str(val_users))
    
    train.createOrReplaceTempView('train')
    test.createOrReplaceTempView('test')
    val.createOrReplaceTempView('val')
    
    
    test_train = spark.sql("SELECT userId, PERCENTILE(timestamp, 0.6) as threshold FROM test GROUP BY userId ")
    val_train = spark.sql("SELECT userId, PERCENTILE(timestamp, 0.6) as threshold FROM val GROUP BY userId ")
    
    test_train.createOrReplaceTempView('test_train')
    val_train.createOrReplaceTempView('val_train')
    
    
    test_train = spark.sql("SELECT t.userId, t.movieId, t.rating, t.timestamp FROM test as t INNER JOIN test_train as tt ON t.userId = tt.userID WHERE t.timestamp <= tt.threshold")
    val_train = spark.sql("SELECT v.userId, v.movieId, v.rating, v.timestamp FROM val as v INNER JOIN val_train as vt ON v.userId = vt.userID WHERE v.timestamp <= vt.threshold")
    
    
    
    train = train.union(test_train)
    train = train.union(val_train)
    
    test = test.subtract(test_train)
    val = val.subtract(val_train)
    
    train.orderBy('userId').show()
    test.orderBy('userId').show()
    val.orderBy('userId').show()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    #movies = spark.read.csv('hdfs:/user/sr6172/movies.csv', header = True, schema = 'movieId INT, title STRING, genres STRING')
    #movie_ratings = movie_ratings.join(movies, on='movieId', how='inner')
    
    #movie_ratings.groupBy("userId").count().show()
    #train=movie_ratings.sampleBy("userId", fractions={ }, seed=1234)
    #train.groupBy("userId").count().show()
    #train.show()
    
    #test=movie_ratings.subtract(train)
    #test.groupBy("userId").count().show()
    
    #test.orderBy('userId').show()
    
    #window = Window.partitionBy('userId').orderBy('')
    #test = test.select('userId','movieId','rating','timestamp', F.row_number().over(window).alias("row_number"))
    
    #test_split = test.filter(test.userId % 2 == 1)
    #val_split = test.filter(test.userId % 2 == 0)
    
    #test_split.show()
    #val_split.show()
    
    
    

    


# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get user netID from the command line
    netID = getpass.getuser()

    # Call our main routine
    main(spark)
    
    

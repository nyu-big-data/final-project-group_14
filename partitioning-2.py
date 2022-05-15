#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 24 15:25:28 2022

@author: shambhavirege
"""
#!/usr/bin/env python
# -*- coding: utf-8 -*-



from pyspark.sql import SparkSession
import sys


def partition(spark, file_path):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    file_path: string path to read and write CSV and Parquet files
    '''
    
    #Reading CSV and Parquet files
    #movie_ratings = spark.read.csv(file_path+'/ratings.csv', header = True ,schema = 'userId INT, movieId INT, rating FLOAT, timestamp DOUBLE')
    movie_ratings = spark.read.parquet(file_path+'/ratings.parquet')
    movie_ratings.show()
    
    movie_ratings.createOrReplaceTempView('movie_ratings')
    
    #Splitting distinct userIds into proportions as follows
    partition = movie_ratings.select('userId').distinct().randomSplit([0.4, 0.3, 0.3], seed = 1234)
    
    
    train_users = tuple(list(x.userId for x in partition[0].collect()))
    val_users = tuple(list(x.userId for x in partition[1].collect()))
    test_users = tuple(list(x.userId for x in partition[2].collect()))
    
    #Making spark data frames for the above splits
    train = spark.sql("SELECT * FROM movie_ratings WHERE userId in "+ str(train_users))
    test = spark.sql("SELECT * FROM movie_ratings WHERE userId in "+ str(test_users))
    val = spark.sql("SELECT * FROM movie_ratings WHERE userId in "+ str(val_users))
    
    train.createOrReplaceTempView('train')
    test.createOrReplaceTempView('test')
    val.createOrReplaceTempView('val')
    
    #Finding 60th percentile of our spark dataframes so as to further divide them into future trends and historic trends
    test_train = spark.sql("SELECT userId, PERCENTILE(timestamp, 0.6) as threshold FROM test GROUP BY userId ")
    val_train = spark.sql("SELECT userId, PERCENTILE(timestamp, 0.6) as threshold FROM val GROUP BY userId ")
    
    test_train.createOrReplaceTempView('test_train')
    val_train.createOrReplaceTempView('val_train')
    
    #Appending historic records to training datasets from validation data and test data each
    test_train = spark.sql("SELECT t.userId, t.movieId, t.rating, t.timestamp FROM test as t INNER JOIN test_train as tt ON t.userId = tt.userID WHERE t.timestamp <= tt.threshold")
    val_train = spark.sql("SELECT v.userId, v.movieId, v.rating, v.timestamp FROM val as v INNER JOIN val_train as vt ON v.userId = vt.userID WHERE v.timestamp <= vt.threshold")
    
    train = train.union(test_train)
    train = train.union(val_train)
    
    #Keeping future trends as our held out data in test and val 
    test = test.subtract(test_train)
    val = val.subtract(val_train)
    
    train.orderBy('userId').groupBy('userId').count().show()
    test.orderBy('userId').groupBy('userId').count().show()
    val.orderBy('userId').groupBy('userId').count().show()
    
    #Writting them to CSVs and Parquets
    #train.write.csv(file_path+'/ratings_train_splits.csv', header=True)
    #val.write.csv(file_path+'/ratings_valid_splits.csv', header=True)
    #test.write.csv(file_path+'/ratings_test_splits.csv', header=True)
    
    train.write.parquet(file_path+'/ratings_train_splits.parquet')
    val.write.parquet(file_path+'/ratings_valid_splits.parquet')
    test.write.parquet(file_path+'/ratings_test_splits.parquet')
    
    
# Only enter this block if we're in main
if __name__ == "__main__":

    # Create Spark session object
    spark = SparkSession.builder.appName('data_partition').getOrCreate()

    # Get file path for the dataset to split
    file_path = sys.argv[1]

    # Calling the split function
    partition(spark, file_path)
    
    

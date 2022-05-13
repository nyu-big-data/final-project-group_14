#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 20:38:20 2022

@author: shambhavirege
"""

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.mllib.evaluation import RankingMetrics
import sys 

def main(spark, file_path):
    
    
    #Reading train, val and test CSVs or Parquet files.
    
    train_ratings = spark.read.csv(file_path+'/ratings_train_splits.csv', header = True ,schema = 'userId INT, movieId INT, rating FLOAT, timestamp INT')
    val_ratings = spark.read.csv(file_path+'/ratings_valid_splits.csv', header = True ,schema = 'userId INT, movieId INT, rating FLOAT, timestamp INT')
    test_ratings = spark.read.csv(file_path+'/ratings_test_splits.csv', header = True ,schema = 'userId INT, movieId INT, rating FLOAT, timestamp INT')
    ratings = spark.read.csv(file_path+'/ratings.csv', header = True ,schema = 'userId INT, movieId INT, rating FLOAT, timestamp INT')
    
    #train_ratings = spark.read.parquet(file_path+'/ratings_train_splits.parquet')
    #val_ratings = spark.read.parquet(file_path+'/ratings_valid_splits.parquet')
    #test_ratings = spark.read.parquet(file_path+'/ratings_test_splits.parquet')
    #ratings = spark.read.parquet(file_path+'/ratings.parquet')
    
    train_ratings.createOrReplaceTempView('train_ratings')
    val_ratings.createOrReplaceTempView('val_ratings')
    test_ratings.createOrReplaceTempView('test_ratings')
    
    

    # Evaluate the model by computing the RMSE on the test data
    hyper_param_maxIter = [5,10,20,25,30]
    for i in hyper_param_maxIter:
        als = ALS(maxIter=i, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
          coldStartStrategy="drop")
        model = als.fit(train_ratings)
        predictions = model.transform(val_ratings)
        evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                predictionCol="prediction")
        rmse = evaluator.evaluate(predictions)
        print("Root-mean-square error = " + str(rmse))

    # Generate top 10 movie recommendations for each user
    userRecs = model.recommendForAllUsers(10)
    # Generate top 10 user recommendations for each movie
    movieRecs = model.recommendForAllItems(10)

    # Generate top 10 movie recommendations for a specified set of users
    users = ratings.select(als.getUserCol()).distinct().limit(3)
    userSubsetRecs = model.recommendForUserSubset(users, 10)
    # Generate top 10 user recommendations for a specified set of movies
    movies = ratings.select(als.getItemCol()).distinct().limit(3)
    movieSubSetRecs = model.recommendForItemSubset(movies, 10)
    
    userRecs.show()
    movieRecs.show()
    userSubsetRecs.show()
    movieSubSetRecs.show()
    
    

if __name__ == "__main__":
    
    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get user netID from the command line
    file_path = sys.argv[1]
    # Call our main routine
    main(spark, file_path)

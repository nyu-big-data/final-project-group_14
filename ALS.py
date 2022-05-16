#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 20:38:20 2022

@author: shambhavirege
"""

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import explode
from pyspark.sql.functions import udf, col
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
    
    test_users = test_ratings.select('userId').distinct()


    # Evaluate the model 
    
    hyper_param_reg = [0.01]#,0.01,0.1,1]
    hyper_param_rank = [200]#,20,100,200,400]
    for i in hyper_param_reg:
        for j in hyper_param_rank:
            
            als = ALS(maxIter=20, regParam= i, userCol="userId", itemCol="movieId", ratingCol="rating",
              coldStartStrategy="drop", rank = j)
            model = als.fit(train_ratings)
            predictions = model.recommendForUserSubset(test_users, 100)
           
            predictions.createOrReplaceTempView("predictions")
            
            
            predictions = predictions.withColumn("movie_recs",col("recommendations.movieId"))
            predictions.createOrReplaceTempView("predictions")
            
            
            
            groundtruth = test_ratings.groupby("userId").agg(F.collect_list("movieId").alias('groundtruth'))
            groundtruth.createOrReplaceTempView("groundtruth")
            total = spark.sql("SELECT g.userId, g.groundtruth AS groundtruth, p.movie_recs AS predictions FROM groundtruth g INNER JOIN predictions p ON g.userId = p.userId")
            total.createOrReplaceTempView("total")
            
            pandasDF = total.toPandas()
            
            eval_list = []
            for index, row in pandasDF.iterrows():
                eval_list.append((row['predictions'], row['groundtruth']))
            
    
    
            
            #eval_list = total.rdd.map(lambda x: (x.predictions, x.groundtruth)).collect()
            #for row in total.rdd:
        
               #eval_list.append((row.predictions.collect(), row.groundtruth.collect()))
            sc =  SparkContext.getOrCreate()
     
            #Evaluation on val
            predictionAndLabels = sc.parallelize(eval_list)
            metrics = RankingMetrics(predictionAndLabels)
            
            print(metrics.precisionAt(100))
            print(metrics.meanAveragePrecision)
            print(metrics.ndcgAt(100))

    
    # Generate top 10 movie recommendations for each user
    #userRecs = model.recommendForAllUsers(10)
    # Generate top 10 user recommendations for each movie
    #movieRecs = model.recommendForAllItems(10)

    # Generate top 10 movie recommendations for a specified set of users
    #users = ratings.select(als.getUserCol()).distinct().limit(3)
    #userSubsetRecs = model.recommendForUserSubset(users, 10)
    # Generate top 10 user recommendations for a specified set of movies
    #movies = ratings.select(als.getItemCol()).distinct().limit(3)
    #movieSubSetRecs = model.recommendForItemSubset(movies, 10)
    
    #userRecs.show()
    #movieRecs.show()
    #userSubsetRecs.show()
    #movieSubSetRecs.show()
    
    

if __name__ == "__main__":
    
    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get user netID from the command line
    file_path = sys.argv[1]
    # Call our main routine
    main(spark, file_path)

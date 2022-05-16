#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May 15 21:51:25 2022

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
    
    train_ratings = spark.read.csv(file_path+'ratings_train_splits.csv', header = True ,schema = 'userId INT, movieId INT, rating FLOAT, timestamp INT')
    val_ratings = spark.read.csv(file_path+'ratings_valid_splits.csv', header = True ,schema = 'userId INT, movieId INT, rating FLOAT, timestamp INT')
    test_ratings = spark.read.csv(file_path+'ratings_test_splits.csv', header = True ,schema = 'userId INT, movieId INT, rating FLOAT, timestamp INT')
    ratings = spark.read.csv(file_path+'ratings.csv', header = True ,schema = 'userId INT, movieId INT, rating FLOAT, timestamp INT')
    
    #train_ratings = spark.read.parquet(file_path+'/ratings_train_splits.parquet')
    #val_ratings = spark.read.parquet(file_path+'/ratings_valid_splits.parquet')
    #test_ratings = spark.read.parquet(file_path+'/ratings_test_splits.parquet')
    #ratings = spark.read.parquet(file_path+'/ratings.parquet')
    
    train_ratings.createOrReplaceTempView('train_ratings')
    val_ratings.createOrReplaceTempView('val_ratings')
    test_ratings.createOrReplaceTempView('test_ratings')
    sc =  SparkContext.getOrCreate()

    
    hyper_param_reg = [0.01]
    hyper_param_rank = [200]
    for i in hyper_param_reg:
        for j in hyper_param_rank:
            
            als = ALS(maxIter=20, regParam= i, userCol="userId", itemCol="movieId", ratingCol="rating",
              coldStartStrategy="drop", rank = j)
            model = als.fit(train_ratings)
            model.save(sc, file_path)
            
            
            

    
    
if __name__ == "__main__":
    
    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get user netID from the command line
    file_path = sys.argv[1]
    # Call our main routine
    main(spark, file_path)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 16 14:42:56 2022

@author: shambhavirege
"""


from pyspark import SparkContext
from pyspark.sql import SparkSession
import sys 

def main(spark, file_path):
    
    

    train_ratings = spark.read.parquet(file_path+'/ratings_train_splits.parquet')
    val_ratings = spark.read.parquet(file_path+'/ratings_valid_splits.parquet')
    test_ratings = spark.read.parquet(file_path+'/ratings_test_splits.parquet')
    
    
    train_ratings.write.csv(file_path+'/ratings_train_splits.csv')
    val_ratings.write.csv(file_path+'/ratings_valid_splits.csv')
    test_ratings.write.csv(file_path+'/ratings_test_splits.csv')
    
    

if __name__ == "__main__":
    
    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get user netID from the command line
    file_path = sys.argv[1]
    # Call our main routine
    main(spark, file_path)

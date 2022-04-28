#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 28 15:51:27 2022

@author: shambhavirege
"""

#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Template script to connect to Active Spark Session
Usage:
    $ spark-submit lab_3_storage_template_code.py <any arguments you wish to add>
'''


# Import command line arguments and helper functions(if necessary)


# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession



def main(spark):
    '''Main routine for run for Storage optimization template.
    Parameters
    ----------
    spark : SparkSession object

    '''
    
    movie_ratings = spark.read.csv('hdfs:/user/sr6172/ml-latest/ratings.csv', header = True ,schema = 'userId INT, movieId INT, rating FLOAT, timestamp DOUBLE')
    movie_ratings.createOrReplaceTempView('movie_ratings')
    movie_ratings.write.parquet("hdfs:/user/sr6172/ml-latest/ratings.parquet") 

   
    

    #Use this template to as much as you want for your parquet saving and optimizations!




# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part2').getOrCreate()

    #If you wish to command line arguments, look into the sys library(primarily sys.argv)
    #Details are here: https://docs.python.org/3/library/sys.html
    #If using command line arguments, be sure to add them to main function
    

    main(spark)

    #main(spark)
    
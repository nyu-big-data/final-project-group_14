import getpass
import math
import pyspark
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import Row
# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession
from pyspark.mllib.evaluation import RankingMetrics
import sys 
import numpy

def main(spark, file_path):
    
    train_ratings = spark.read.csv(file_path+'/ratings_train_splits.csv', header = True ,schema = 'userId INT, movieId INT, rating FLOAT, timestamp INT')
    val_ratings = spark.read.csv(file_path+'/ratings_valid_splits.csv', header = True ,schema = 'userId INT, movieId INT, rating FLOAT, timestamp INT')
    test_ratings = spark.read.csv(file_path+'/ratings_test_splits.csv', header = True ,schema = 'userId INT, movieId INT, rating FLOAT, timestamp INT')
    
    train_ratings.createOrReplaceTempView('train_ratings')
    val_ratings.createOrReplaceTempView('val_ratings')
    test_ratings.createOrReplaceTempView('test_ratings')
    
    
    
    train_ratings = spark.sql("SELECT movieId, rating FROM train_ratings")
    
    w = Window.partitionBy('movieId')
    top_100 = train_ratings.withColumn("avg_rating", F.avg("rating").over(w)).orderBy("avg_rating", ascending=False).limit(100)
    top_100.show()
    
    
    
    val_ratings = val_ratings.groupBy("userId").agg(F.collect_list("movieId").alias("movieIds"))
    top_100 = top_100.select('movieId').rdd.flatMap(lambda x: x).collect()
    
    eval_list = []
    for row in val_ratings.rdd.collect():
        
        eval_list.append((top_100, row.movieIds))
        
    predictionAndLabels = pyspark.SparkContext.parallelize(eval_list)
    metrics = RankingMetrics(predictionAndLabels)
    print(metrics.meanAveragePrecisionAt(100))
    print(metrics.meanAveragePrecision)
    
    
    
    
    
    
    
    
    
    
    
    
    

if __name__ == "__main__":
    
    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get user netID from the command line
    file_path = sys.argv[1]
    # Call our main routine
    main(spark, file_path)

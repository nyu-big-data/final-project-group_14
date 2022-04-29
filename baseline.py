from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.mllib.evaluation import RankingMetrics
import sys 

def main(spark, file_path):
    
    
    #Reading train, val and test CSVs or Parquet files.
    
    #train_ratings = spark.read.csv(file_path+'/ratings_train_splits.csv', header = True ,schema = 'userId INT, movieId INT, rating FLOAT, timestamp INT')
    #val_ratings = spark.read.csv(file_path+'/ratings_valid_splits.csv', header = True ,schema = 'userId INT, movieId INT, rating FLOAT, timestamp INT')
    #test_ratings = spark.read.csv(file_path+'/ratings_test_splits.csv', header = True ,schema = 'userId INT, movieId INT, rating FLOAT, timestamp INT')
    
    train_ratings = spark.read.parquet(file_path+'/ratings_train_splits.parquet')
    val_ratings = spark.read.parquet(file_path+'/ratings_valid_splits.parquet')
    test_ratings = spark.read.parquet(file_path+'/ratings_test_splits.parquet')
    
    train_ratings.createOrReplaceTempView('train_ratings')
    val_ratings.createOrReplaceTempView('val_ratings')
    test_ratings.createOrReplaceTempView('test_ratings')
    
    train_ratings = spark.sql("SELECT movieId, rating FROM train_ratings")
    
    #Partitioning the data to find average ratings for each movie in train ratings
    w = Window.partitionBy('movieId')
    top_100 = train_ratings.withColumn("avg_rating", F.avg("rating").over(w)).orderBy("avg_rating", ascending=False).limit(100)
    top_100.show()
    
    
    #Storing top 100 movieIds as a list
    val_ratings = val_ratings.groupBy("userId").agg(F.collect_list("movieId").alias("movieIds"))
    top_100 = top_100.select('movieId').rdd.flatMap(lambda x: x).collect()
    
    
    eval_list = []
    for row in val_ratings.rdd.collect():
        
        eval_list.append((top_100, row.movieIds))

    sc =  SparkContext.getOrCreate()
     
    #Evaluation on val
    predictionAndLabels = sc.parallelize(eval_list)
    metrics = RankingMetrics(predictionAndLabels)
    
    print(metrics.meanAveragePrecisionAt(100))
    print(metrics.meanAveragePrecision)
    print(metrics.precisionAt(100))
    
    test_ratings = test_ratings.groupBy("userId").agg(F.collect_list("movieId").alias("movieIds"))
    
    eval_list2 = []
    for row in test_ratings.rdd.collect():
        
        eval_list2.append((top_100, row.movieIds))
        
    #Evaluation on test    
    predictionAndLabels = sc.parallelize(eval_list2)
    metrics2 = RankingMetrics(predictionAndLabels)
    
    print(metrics2.meanAveragePrecisionAt(100))
    print(metrics2.meanAveragePrecision)
    print(metrics2.precisionAt(100))
    

if __name__ == "__main__":
    
    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get user netID from the command line
    file_path = sys.argv[1]
    # Call our main routine
    main(spark, file_path)

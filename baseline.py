import getpass
import math
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import Row
# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession
def main(spark, file_path):
    
    train_ratings = spark.read.csv(file_path+'/ratings_train_splits.csv', header = True ,schema = 'userId INT, movieId INT, rating FLOAT, timestamp INT')
    val_ratings = spark.read.csv(file_path+'/ratings_val_splits.csv', header = True ,schema = 'userId INT, movieId INT, rating FLOAT, timestamp INT')
    test_ratings = spark.read.csv(file_path+'/ratings_test_splits.csv', header = True ,schema = 'userId INT, movieId INT, rating FLOAT, timestamp INT')
    
    train_ratings.createOrReplaceTempView('train_ratings')
    val_ratings.createOrReplaceTempView('val_ratings')
    test_ratings.createOrReplaceTempView('test_ratings')
    
    
    
    train_ratings = spark.sql("SELECT movieId, rating FROM train_ratings")
    
    w = Window.partitionBy('movieId')
    top_100 = train_ratings.withColumn("avg_rating", F.avg("rating").over(w)).orderBy("avg_rating", ascending=False).limit(100)
    top_100.show()
    
    
    
    val_ratings.groupBy("userId").agg(F.collect_list("movieId").alias("movieIds")).show()
    
    top_100 = top_100.select('movieId').rdd.flatMap(lambda x: x).collect()
    
    
    
    
    
    
    
    
    

if __name__ == "__main__":
    
    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get user netID from the command line
    netID = getpass.getuser()

    # Call our main routine
    main(spark)

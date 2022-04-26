import getpass
import math
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import Row
# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession
def main(spark):
    movie_ratings = spark.read.csv('hdfs:/user/sa6523/ratings.csv', header = True ,schema = 'userId INT, movieId INT, rating FLOAT, timestamp INT')
    w = Window.partitionBy('movieId')
    movie_ratings.withColumn("avg_rating", F.avg("rating").over(w)).orderBy("avg_rating", ascending=False).show()

if __name__ == "__main__":
    
    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get user netID from the command line
    netID = getpass.getuser()

    # Call our main routine
    main(spark)

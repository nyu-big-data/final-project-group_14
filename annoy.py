#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May 15 21:42:17 2022

@author: shambhavirege
"""


import sys
import psutil
import math
from annoy import AnnoyIndex

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window




def main(spark, path):
    
    als_model = ALSModel.load(path + "ALSmodel/ALS")

    # Build annoy tree
    item_factors = als_model.itemFactors

    window = Window.orderBy('id')
    
    item_factors = item_factors.withColumn('annoy_id', F.row_number().over(window))

    annoy_index_map = item_factors.select('id', 'annoy_id')     
    item_factors = item_factors.select('annoy_id', 'features')  

    t = AnnoyIndex(als_model.rank, 'dot')

    for row in item_factors.collect():
        t.add_item(row.annoy_id, row.features)

    t.set_seed(1234)
    t.build(30)
    
    t.save(path + "tree")

    annoy_index_map.write.parquet(path + "index_map")
    
    print('done')

    
    
    
    

    
    


if __name__ == '__main__':

    # Path to load ALS model
    als_model_path = sys.argv[1]

    

    # Create Spark session
    # memory = f'{math.floor(psutil.virtual_memory()[1]*.9) >> 30}g'
    memory = '10g'
    spark = (SparkSession.builder
             .appName('build_annoy_tree')
             .master('yarn')
             .config('spark.executor.memory', memory)
             .config('spark.driver.memory', memory)
             .getOrCreate())
    # spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    main(spark, path)
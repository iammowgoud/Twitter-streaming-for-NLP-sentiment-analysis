from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
import pandas as pd

import sys
import requests

import re

TCP_PORT = 1003


def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']


def add_row(df, row):
    df.loc[-1] = row
    df.index = df.index + 1
    return df.sort_index()


def process_rdd(time, rdd):
    try:
        lis = rdd.collect()
        if (len(lis) > 0):
            df = pd.DataFrame(
                columns=['search_category', 'time', 'tweet_text'])
            category = lis[0].split("--", 1)[0]
            text = lis[0].split("--", 1)[1]
            add_row(df, [category, str(time), text])

            # Create file unless exists, otherwise append
            # Add header if file is being created, otherwise skip it
            with open("./data/ny.csv", 'a') as f :
                df.to_csv(f, header=f.tell() == 0, index=False)
            print(f'========= APPENDED ONE TWEET TO FILE: {category}')
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")

# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# create the Streaming Context from the above spark context with interval size 10 seconds
ssc = StreamingContext(sc, 2)

# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 1235
dataStream = ssc.socketTextStream("localhost", TCP_PORT)

dataStream.foreachRDD(process_rdd)

# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()

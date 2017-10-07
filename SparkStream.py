from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys

# create spark configuration
conf = SparkConf().setMaster("local[*]").setAppName("TwitterStreamApp")
sc = SparkContext(conf= conf)
sc.setLogLevel("ERROR")


# Lazily instantiated global instance of SparkSession
def get_sql_context_instance(spark_context):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SQLContext(spark_context)
    return globals()["sparkSessionSingletonInstance"]


def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        # Get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)
        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
        # create a DF from the Row RDD
        hashtags_df = sql_context.createDataFrame(row_rdd)
        # Register the dataframe as table
        hashtags_df.registerTempTable("hashtags")
        # get the top 10 hashtags from the table using SQL and print them
        hashtag_counts_df = sql_context.sql("select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 3")
        # print the data frame
        hashtag_counts_df.show()
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

# create the Streaming Context from the above spark context with window size 15 seconds
ssc = StreamingContext(sc, 15)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 10000
dataStream = ssc.socketTextStream("localhost", 10000)

#streaming tweets in - splitting it up into workds - filtering out words which ado not begin with #
hashtags = dataStream.flatMap(lambda line: line.split(" ")).filter(lambda hashword: hashword.startswith('#'))
#making pairs like (hashtag, 1)
pairs = hashtags.map(lambda word: (word, 1))
#adding count of each hashtag to last count
HashCounts = pairs.reduceByKey(lambda x, y: x + y)
#Updating counts for each hashtag over their previous session counts
tags_totals = HashCounts.updateStateByKey(aggregate_tags_count)
#processing each RDD generated
tags_totals.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination()
# import session
import time

from pyspark.sql import SparkSession
from pyspark import SparkConf

# main spark program
# init application

if __name__ == '__main__':
    # init session
    # set cofigs
    # https://spark.apache.org/docs/latest/configuration.html
    # spark default = spark.sql.maxPartitiosBytes = 128mb
    # hdfs default = 128mb
    # s3, blob, gcs = {64mb} to {128mb}

    # each devide & subscription file around [30kb]
    spark = SparkSession \
        .builder \
        .master("local[4]") \
        .appName("small-files-problem") \
        .config("spark.sql.files.maxPartitionBytes", "128mb") \
        .getOrCreate()
    #.config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
    #.config("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false") \
    #.config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "10kb") \
    #.config("spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled", "false") \

    # show configuration parameters
    print(SparkConf().getAll())

    # set log level & default paralelism
    spark.sparkContext.setLogLevel("WARN")
    print("default parallelism: {}".format(spark.sparkContext.defaultParallelism))
    print("max partition bytes: {}".format(SparkConf().getAll()[6][1]))

    # loading json path
    # get_json = "/home/clay/workspace/spark-course/docs/files/json/*.json"

    # loading 1 file
    # get_device_file = "/home/clay/workspace/spark-course/docs/files/device/device_2022_6_7_19_39_24.json"
    # get_subscription_file = "/home/clay/workspace/spark-course/docs/files/subscription/subscription_2022_6_7_19_39_21.json"

    # laoding = [all files = 5 files]

    get_device_file = "/home/clay/workspace/spark-course/docs/files/device/*.json"
    get_subscription_file = "/home/clay/workspace/spark-course/docs/files/subscription/*.json"

    # read devide data
    start = time.time()
    df_device = spark.read \
        .format("json") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .json(get_device_file)
    df_device.count()
    print("number of partitions for [device]: {}".format(df_device.rdd.getNumPartitions()))
    print(f"total time spend in [secs]: {round(time.time() - start, 2)}")

    # read get_subscription_file data
    start = time.time()
    df_subscription = spark.read \
        .format("json") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .json(get_subscription_file)
    df_subscription.count()
    print("number of partitions for [subscription]: {}".format(df_subscription.rdd.getNumPartitions()))
    print(f"total time spend in [secs]: {round(time.time() - start, 2)}")

# read path json data
# start = time.time()
# df_json = spark.read \
#     .format("json") \
#     .option("inferSchema", "true") \
#     .option("header", "true") \
#     .json(get_json)
# print(df_json.count())
# print("number of partitions for [json]: {}".format(df_json.rdd.getNumPartitions()))
# print(f"total time spend in [secs]: {round(time.time() - start, 2)}")

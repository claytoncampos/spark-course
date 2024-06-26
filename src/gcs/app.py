from pyspark.sql import SparkSession
from pyspark import SparkConf

spark = SparkSession \
    .builder \
    .appName("app-gcs-json") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "<arquivo-credential.json>") \
    .getOrCreate()

print(SparkConf().getAll())
spark.sparkContext.setLogLevel("INFO")

df_users = spark.read.json("gs://<path>/*.json")
df_users.show()
# import session
from pyspark.sql import SparkSession
from pyspark import SparkConf


spark = SparkSession.builder.appName("etl-job-users").getOrCreate()

#configs
print(spark)
print(SparkConf().getAll())
spark.sparkContext.setLogLevel("INFO")

# extract {E}
users_filepath = "/src/readme.md"
df_users = spark.read.text(users_filepath)
df_users.show()



# import libraries & init spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('app_elt').getOrCreate()

# load one archive
df_device = spark.read.json('/home/clay/workspace/firstProject/docs/files/device/device_2022_6_7_19_39_24.json')

# load all files using regex
df_all_device = spark.read.json('/home/clay/workspace/firstProject/docs/files/device/*.json')

# count rows all files

print(df_all_device.count())

# schema
df_device.printSchema()

# columns
print(df_device.columns)

# rows
print(df_device.count())

# show
df_device.show()

# select cols

df_device.select("manufacturer", "model", "platform").show()
df_device.selectExpr("manufacturer", "model", "platform as type").show()


# filter
df_device.filter(df_device.manufacturer == "Xiamomi").show()

# group by -- até o count é tranformação ao usar o show é a ação
df_device.groupBy("manufacturer").count().show()

# created new df grouped by 'manufacturer'
df_grouped_manufacturer = df_device.groupBy("manufacturer").count()
df_grouped_manufacturer.show()
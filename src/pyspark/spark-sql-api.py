from pyspark.sql import SparkSession

# init & instantiate spark session
spark = SparkSession.builder.appName("spark-sql").getOrCreate()

# get_device = spark.read.json("/home/clay/workspace/firstProject/docs/files/device")

# data import
spark.sql("""
    CREATE TEMPORARY VIEW vw_device
    USING org.apache.spark.sql.json
    OPTIONS (path "/home/clay/workspace/firstProject/docs/files/device/*.json")
""")


spark.sql("""
    CREATE TEMPORARY VIEW vw_subscription
    USING org.apache.spark.sql.json
    OPTIONS (path "/home/clay/workspace/firstProject/docs/files/subscription/*.json")
""")

print(spark.catalog.listTables())


# select data

spark.sql("""
SELECT * FROM vw_device LIMIT 10;
""").show()

spark.sql("""
SELECT * FROM vw_subscription LIMIT 10;
""").show()


# new dataframe = {join}
join_datasets = spark.sql("""
SELECT  dev.user_id,
        dev.model,
        subs.payment_method,
        subs.plan
FROM vw_device AS dev
INNER JOIN vw_subscription AS subs
ON dev.user_id = subs.user_id
""")

# info = executar a ação mostrar o dado

join_datasets.show()
join_datasets.printSchema()
print(join_datasets.count())

""""
*****************************
Pandas API on Spark {PyArrow}
* PySpark
* Koalas
* PySpark.Pandas

*****************************
Not 100% Compatible Yet {WIP}
https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/index.html

*****************************
Recommended Path
Already Spark Developer = PySpark’s API
Pandas Developer = Pandas API on Spark {Does Not Support Structured Streaming Officially}

*****************************
Default Index Type
22/12/29 20:26:56 WARN WindowExec: No Partition Defined for Window Operation! Moving All Data to a Single Partition

Spark DataFrame is Converted into Pandas-on-Spark DataFrame
sequence
distributed-sequence
distributed
https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/options.html#default-index-type
"""

# import & set configs
from pyspark.sql import SparkSession
builder = SparkSession.builder.appName("app-with-pandas")
# arrow engine colunnar on memory to serializer data
builder = builder.config("spark.sql.execution.arrow.pyspark.enable", "true")
builder.getOrCreate()
print(builder)

# pandas on spark scalabe using executor, se usar pandas normal ele não usa a escalabilidade roda apenas no driver
import pyspark.pandas as ps

# read files
get_device = ps.read_json("/home/clay/workspace/firstProject/docs/files/device/*.json")
get_subscription = ps.read_json("/home/clay/workspace/firstProject/docs/files/subscription/*.json")

# pandas there is no concept of show like spark, you need to use print
print(get_device)
print(get_subscription)

# statistics
get_device.info()
get_subscription.info()

# get plan execution
get_device.spark.explain(mode="formatted")
get_subscription.spark.explain(mode="formatted")
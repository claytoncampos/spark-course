```shell
spark-shell --help
pyspark --help
http://localhost:4040/jobs/
```

```scala
val readmeFile = spark.read.text("//home/clay/workspace/firstProject/src/readme.md")
readmeFile.count() 
readmeFile.first()
```

```pyspark
readmeFile = spark.read.text("/home/clay/workspace/firstProject/src/readme.md")
readmeFile.count() 
readmeFile.first()
```
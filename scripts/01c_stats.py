from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("stats").getOrCreate()
clean = spark.read.parquet("hdfs:///projet/reviews/clean")
print("Avis apres nettoyage :", clean.count())
clean.groupBy("score").count().orderBy("score").show()
clean.groupBy("label").count().show()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("compact").getOrCreate()
spark.read.text("hdfs:///projet/reviews/clean_tsv").coalesce(4).write.mode("overwrite").text("hdfs:///projet/reviews/clean_tsv4")

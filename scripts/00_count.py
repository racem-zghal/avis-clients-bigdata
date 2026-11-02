from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("count").getOrCreate()
df = (spark.read.option("header", "true")
      .option("multiLine", "true").option("escape", '"')
      .csv("hdfs:///projet/reviews/raw/Reviews.csv"))
print("Avis avant nettoyage :", df.count())

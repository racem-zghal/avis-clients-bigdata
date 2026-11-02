from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("export").getOrCreate()
clean = spark.read.parquet("hdfs:///projet/reviews/clean")
pred = spark.read.parquet("hdfs:///projet/reviews/predictions")

out = (clean.join(pred, "Id", "left")
       .select("Id", "ProductId", "UserId", "score", "summary", "review", "sentiment_pred"))

out.coalesce(1).write.mode("overwrite").json("hdfs:///projet/reviews/export_json")
print("Documents exportes :", out.count())
print("Avec prediction :", out.filter("sentiment_pred IS NOT NULL").count())

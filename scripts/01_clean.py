from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("clean").getOrCreate()

df = (spark.read.option("header", "true")
      .option("multiLine", "true").option("escape", '"')
      .csv("hdfs:///projet/reviews/raw/Reviews.csv"))

clean = (df.select("Id", "ProductId", "UserId",
                   F.col("Score").cast("int").alias("score"),
                   F.col("Time").cast("long").alias("time"),
                   F.col("Summary").alias("summary"),
                   F.col("Text").alias("review"))
  .dropna(subset=["Id", "ProductId", "score", "review"])
  .withColumn("clean_text", F.lower(F.col("review")))
  .withColumn("clean_text", F.regexp_replace("clean_text", r"<[^>]+>", " "))
  .withColumn("clean_text", F.regexp_replace("clean_text", r"[^a-z\s']", " "))
  .withColumn("clean_text", F.regexp_replace("clean_text", r"\s+", " "))
  .filter(F.length("clean_text") > 0)
  .dropDuplicates(["UserId", "ProductId", "time", "clean_text"])
  .withColumn("label", F.when(F.col("score") >= 4, 1.0)
                        .when(F.col("score") <= 2, 0.0)))

clean.write.mode("overwrite").parquet("hdfs:///projet/reviews/clean")

(clean.select("Id", "score", "clean_text")
  .write.mode("overwrite").option("sep", "\t").csv("hdfs:///projet/reviews/clean_tsv"))

print("Avant :", df.count(), "| Apres :", clean.count())
clean.groupBy("score").count().orderBy("score").show()

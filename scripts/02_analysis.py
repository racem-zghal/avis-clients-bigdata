import time
from pyspark.sql import SparkSession, functions as F

STOP = set("the and for are but not you all was this that with have had has from they its out one our can get got them were what when will more than just also very would there their been which about into your like these it's don't i've i'm that's they're".split())

spark = SparkSession.builder.appName("analysis").getOrCreate()
stop = spark.sparkContext.broadcast(STOP)
clean = spark.read.parquet("hdfs:///projet/reviews/clean")

t0 = time.time()
words = clean.rdd.flatMap(lambda r: [w for w in r.clean_text.split() if len(w) > 2 and w not in stop.value])
counts = words.map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)
top = counts.takeOrdered(20, key=lambda x: -x[1])
t1 = time.time()
print("Temps word count Spark (s) :", round(t1 - t0, 1))
for w, c in top:
    print(w, c)

top_products = (clean.groupBy("ProductId")
    .agg(F.avg("score").alias("avg_score"), F.count("*").alias("n"))
    .filter("n >= 50").orderBy(F.desc("avg_score")).limit(10))
top_products.show()

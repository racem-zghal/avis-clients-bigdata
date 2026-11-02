from pyspark.sql import SparkSession

STOP = set("the and for are but not you all was this that with have had has from they its out one our can get got them were what when will more than just also very would there their been which about into your like these it's don't i've i'm that's they're".split())

spark = SparkSession.builder.appName("inverted_index").getOrCreate()
stop = spark.sparkContext.broadcast(STOP)
clean = spark.read.parquet("hdfs:///projet/reviews/clean")

pairs = clean.rdd.flatMap(lambda r: [(w, r.Id) for w in set(r.clean_text.split()) if len(w) > 2 and w not in stop.value])

def seq(acc, x):
    c, l = acc
    if len(l) < 100:
        return (c + 1, l + [x])
    return (c + 1, l)

def comb(a, b):
    return (a[0] + b[0], (a[1] + b[1])[:100])

index = pairs.aggregateByKey((0, []), seq, comb).filter(lambda kv: kv[1][0] >= 5)
df = index.map(lambda kv: (kv[0], kv[1][0], kv[1][1])).toDF(["word", "n_reviews", "review_ids"])
df.write.mode("overwrite").json("hdfs:///projet/reviews/inverted_index")
print("Mots indexes :", df.count())
df.filter("word = 'delivery'").show(truncate=False)

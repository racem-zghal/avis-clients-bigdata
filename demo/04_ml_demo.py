import sys
import time
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

frac = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0

spark = SparkSession.builder.appName("ml").getOrCreate()
clean = spark.read.parquet("hdfs:///projet/reviews/clean")

data = clean.filter(F.col("label").isNotNull()).select("Id", "clean_text", "label")
if frac < 1.0:
    data = data.sample(False, frac, seed=42)
data = data.cache()

n = data.count()
n_neg = data.filter("label = 0").count()
neg_share = n_neg / float(n)
print("Avis utilises :", n, "| part des negatifs :", round(neg_share, 3))

data = data.withColumn("w", F.when(F.col("label") == 0, 1.0 - neg_share).otherwise(neg_share))
train, test = data.randomSplit([0.8, 0.2], seed=42)

pipe = Pipeline(stages=[
    Tokenizer(inputCol="clean_text", outputCol="tok"),
    StopWordsRemover(inputCol="tok", outputCol="tok2"),
    CountVectorizer(inputCol="tok2", outputCol="tf", vocabSize=10000, minDF=5),
    IDF(inputCol="tf", outputCol="features"),
    LogisticRegression(featuresCol="features", labelCol="label", weightCol="w", maxIter=20)])

t0 = time.time()
model = pipe.fit(train)
print("Temps entrainement (s) :", round(time.time() - t0, 1))

pred = model.transform(test).cache()

auc = BinaryClassificationEvaluator(labelCol="label").evaluate(pred)
f1 = MulticlassClassificationEvaluator(labelCol="label", metricName="f1").evaluate(pred)
print("AUC :", round(auc, 4))
print("F1 pondere :", round(f1, 4))

pred.groupBy("label", "prediction").count().orderBy("label", "prediction").show()

tp = pred.filter("label = 0 AND prediction = 0").count()
fp = pred.filter("label = 1 AND prediction = 0").count()
fn = pred.filter("label = 0 AND prediction = 1").count()
precision = tp / float(tp + fp) if (tp + fp) > 0 else 0.0
recall = tp / float(tp + fn) if (tp + fn) > 0 else 0.0
print("Classe negative - precision :", round(precision, 3), "| rappel :", round(recall, 3))

(model.transform(data)
      .select("Id", F.col("prediction").cast("int").alias("sentiment_pred"))
      .write.mode("overwrite").parquet("hdfs:///projet/demo/predictions"))
print("Predictions ecrites")

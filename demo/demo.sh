#!/bin/bash
export PYSPARK_PYTHON=python3
cd /root/projet
pause() { read -r -p "[Entree pour continuer] " x; }

echo "=== 1. Stockage HDFS ==="
hdfs dfs -ls -h /projet/reviews/raw
hdfs fsck /projet/reviews/raw/Reviews.csv -files -blocks | grep -E "block\(s\)|blk_|Status|Average block replication|Number of data-nodes"
pause

echo "=== 2. Nettoyage avec Spark ==="
spark-submit --master local[2] 00_count.py 2>/dev/null
spark-submit --master local[2] 01c_stats.py 2>/dev/null
pause

echo "=== 3. MapReduce : word count avec combiner ==="
hdfs dfs -rm -r -f /projet/demo/wc_mr > /dev/null 2>&1
cd /root/projet/mapreduce
( time hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar -input /projet/reviews/clean_tsv4 -output /projet/demo/wc_mr -mapper mapper.py -combiner reducer.py -reducer reducer.py -file mapper.py -file reducer.py ) 2> /tmp/demo_log.txt
grep -E "^real|Map output records|Combine input records|Combine output records|Reduce input records" /tmp/demo_log.txt
hdfs dfs -cat /projet/demo/wc_mr/part-* | sort -t$'\t' -k2 -nr | head -10
echo "--- Mesure enregistree sans combiner ---"
grep -E "^real|Map output records|Reduce input records" /root/projet/mapreduce/log_simple.txt
cd /root/projet
pause

echo "=== 4. Spark : word count et meilleurs produits ==="
time spark-submit --master yarn --deploy-mode client 02_analysis.py 2>/dev/null
pause

echo "=== 5. Index inverse dans HDFS ==="
hdfs dfs -ls -h /projet/reviews/inverted_index | head -5
pause

echo "=== 6. Modele MLlib ==="
spark-submit --master local[2] --driver-memory 2g 04_ml_demo.py 2>/dev/null
pause

echo "=== Suite : MongoDB, dans la fenetre Windows ==="

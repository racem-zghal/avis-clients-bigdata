# Analyse d'avis clients avec une architecture Big Data

Mini-projet du module Big Data (ENSI, II3). Pipeline complet de stockage, traitement et interrogation de 568 454 avis clients Amazon (Fine Food Reviews).

## Problème

Une plateforme de e-commerce reçoit des centaines de milliers d'avis en texte libre. Personne ne peut les lire un par un, mais ils disent quels produits plaisent, quels sujets reviennent et quel est le sentiment général. Le projet construit une chaîne qui stocke ces avis de façon distribuée, les nettoie, les analyse, apprend à classer leur sentiment et les rend interrogeables.

## Données

- Jeu : Amazon Fine Food Reviews (Kaggle), fichier `Reviews.csv`, 287 Mo.
- Colonnes utilisées : `Id`, `ProductId`, `UserId`, `Score`, `Time`, `Summary`, `Text`.
- Les données ne sont pas incluses dans ce dépôt : elles sont à télécharger depuis Kaggle.

## Architecture

```
Reviews.csv ──► HDFS (brut) ──► Spark : nettoyage ──► HDFS (Parquet + texte tabulé)
                                                          │
              ┌───────────────────────┬───────────────────┤
              ▼                       ▼                   ▼
   MapReduce (Hadoop Streaming)   Spark (RDD / DataFrame)   Spark MLlib
   word count + combiner          Top-n, index inversé      TF-IDF + régression logistique
              └───────────────────────┴───────────────────┘
                                      ▼
                         MongoDB (avis + index inversé)
```

## Technologies et justification

| Technologie | Rôle | Justification |
|---|---|---|
| HDFS | Stockage | Fichier découpé en blocs de 128 Mo, répliqué, traitement au plus près des données |
| MapReduce (Hadoop Streaming) | Word count avec et sans combiner | Illustre les patterns de récapitulation ; le combiner réduit le volume du shuffle |
| Spark | Nettoyage, analyses, index inversé | Étapes enchaînées en mémoire, code court, DataFrames |
| Spark MLlib | Classification du sentiment | Entraînement itératif sur des données gardées en mémoire |
| MongoDB | Stockage final et requêtes | Un avis est un document JSON semi-structuré ; requêtes par produit et par mot |

Environnement : image Docker `liliasfaxi/spark-hadoop:hv-2.7.2` (Hadoop 2.7.2, Spark 2.2.0, Python 3.5.2), un maître et deux esclaves, plus un conteneur `mongo:7`.

## Résultats mesurés

### Stockage HDFS

- 300 904 694 octets répartis en 3 blocs (128 Mo, 128 Mo, environ 31 Mo).
- Réplication de 2 : le cluster n'a que 2 DataNodes. Statut `HEALTHY`.

### Nettoyage

- 568 454 avis avant, 567 118 après.
- Répartition des notes : 1 étoile 51 957, 2 étoiles 29 751, 3 étoiles 42 550, 4 étoiles 80 535, 5 étoiles 362 325.
- Étiquettes : positif (4 et 5 étoiles) 442 860, négatif (1 et 2 étoiles) 81 708, avis à 3 étoiles exclus du modèle.

### MapReduce : effet du combiner

| | Sans combiner | Avec combiner |
|---|---|---|
| Couples émis par les mappers | 23 442 332 | 23 442 332 |
| Couples reçus par les reducers | 23 442 332 | 389 901 |
| Temps total de la commande | 35,6 s | 21,5 s |

Les deux jobs donnent un résultat identique (vérifié avec `cmp`). Chaque temps provient d'une seule exécution.

### Spark

- Même Top 20 de mots que MapReduce (good 199 658, taste 172 302, great 166 508, coffee 165 802, product 150 854…).
- Word count seul : 10,0 s en mode local et 10,8 s sur YARN, hors démarrage de l'application.
- Commande complète sur YARN, avec en plus le Top 10 des produits : 24,2 s.

### Index inversé

- 41 569 mots indexés, présents dans au moins 5 avis. Pour chaque mot : le nombre d'avis et 100 identifiants d'avis.
- Exemple : `delivery` apparaît dans 7 861 avis.

### Modèle de sentiment (régression logistique sur TF-IDF, classes pondérées)

| | 10 % des données | 100 % des données |
|---|---|---|
| Avis utilisés | 52 599 | 524 568 |
| Temps d'entraînement | 4,4 s | 21,4 s |
| AUC | 0,891 | 0,954 |
| F1 pondéré | 0,894 | 0,913 |
| Précision (classe négative) | 0,639 | 0,649 |
| Rappel (classe négative) | 0,705 | 0,887 |

Matrice de confusion sur l'échantillon de test (104 602 avis) :

| | Prédit négatif | Prédit positif |
|---|---|---|
| Vraiment négatif | 14 527 | 1 847 |
| Vraiment positif | 7 843 | 80 385 |

### MongoDB

- 567 118 documents dans `reviews`, 41 569 dans `inverted`.
- Prédictions : 112 288 négatifs, 412 280 positifs, 42 550 sans prédiction (avis à 3 étoiles).
- Exemple : le produit `B000NMJWZO` compte 542 avis, avec une moyenne de 4,88.

## Limites

- Le jeu de données est modeste pour du Big Data (environ 300 Mo) : l'architecture est conçue pour passer à l'échelle, mais le gain de Spark sur MapReduce n'est pas mesurable à cette taille.
- Les temps proviennent d'une exécution unique, sur un petit cluster.
- Le modèle est un « sac de mots » : il ne voit ni la négation ni l'ironie. La pondération des classes donne un bon rappel au prix d'une précision de 0,65.
- La liste de mots vides du mapper est écrite à la main et laisse passer quelques mots peu informatifs (other, some, really).
- MongoDB tourne sans authentification, ce qui convient à une démonstration locale mais pas à un déploiement.

## Contenu du dépôt

```
README.md
scripts/      00_count.py  01_clean.py  01b_compact.py  01c_stats.py
              02_analysis.py  03_inverted_index.py  04_ml.py  05_export.py
mapreduce/    mapper.py  reducer.py
mongo/        queries.js
demo/         demo.sh  04_ml_demo.py
docs/         log_simple.txt  log_combiner.txt
```

## Reproduire le pipeline

Les scripts ont été exécutés à plat depuis `/root/projet` dans le conteneur `hadoop-master`. Pour les rejouer :

```
docker cp scripts/. hadoop-master:/root/projet/
docker cp mapreduce hadoop-master:/root/projet/
docker cp demo/. hadoop-master:/root/projet/
```

Dans le conteneur, après `./start-hadoop.sh` :

```
hdfs dfs -mkdir -p /projet/reviews/raw
hdfs dfs -put /root/Reviews.csv /projet/reviews/raw/
cd /root/projet
export PYSPARK_PYTHON=python3
spark-submit --master local[2] 01_clean.py
spark-submit --master local[2] 01b_compact.py
cd mapreduce
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar -input /projet/reviews/clean_tsv4 -output /projet/reviews/wc_mr_combiner -mapper mapper.py -combiner reducer.py -reducer reducer.py -file mapper.py -file reducer.py
cd ..
spark-submit --master yarn --deploy-mode client 02_analysis.py
spark-submit --master yarn --deploy-mode client 03_inverted_index.py
spark-submit --master local[2] --driver-memory 2g 04_ml.py
spark-submit --master local[2] --driver-memory 2g 05_export.py
```

Le module Python `numpy` est nécessaire à MLlib : `apt-get install -y python3-numpy`.

L'import dans MongoDB se fait avec `mongoimport` sur les fichiers exportés (`reviews.json`, `inverted.json`), puis les requêtes de `mongo/queries.js`.

## Répartition du travail

À compléter par l'équipe : nom de chaque membre et sa part (environnement et HDFS, MapReduce et Spark, modèle et MongoDB), sachant que chacun a exécuté et compris l'ensemble du code.

## Usage de l'IA générative

Projet réalisé en Zone 3 de la charte de l'école. L'assistant Claude a servi à la conception, au débogage et à l'explication des résultats. La fiche de déclaration d'usage signée et le journal des prompts sont remis avec le dépôt à l'enseignant.

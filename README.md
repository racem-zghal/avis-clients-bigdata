# Analyse d'avis clients : un pipeline Big Data

Mini-projet du module Big Data (ENSI, II3). Une chaîne complète de stockage, de traitement et d'interrogation de 568 454 avis clients Amazon, construite avec HDFS, MapReduce, Spark, Spark MLlib et MongoDB, complétée par un tableau de bord Streamlit.

## Présentation

### Problème

Une plateforme de e-commerce reçoit des centaines de milliers d'avis rédigés en texte libre. Personne ne peut les lire un par un, alors qu'ils indiquent quels produits plaisent, quels sujets reviennent et quel est le sentiment général des clients.

### Objectif

Construire une architecture qui :

1. stocke les avis de façon distribuée et tolérante aux pannes ;
2. les nettoie et en extrait des statistiques à grande échelle ;
3. apprend à classer chaque avis comme positif ou négatif à partir de son texte ;
4. rend le résultat interrogeable rapidement par produit, par mot ou par sentiment.

### Jeu de données

- Amazon Fine Food Reviews (Kaggle), fichier `Reviews.csv` de 287 Mo.
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
                         MongoDB (avis enrichis + index inversé)
                                      ▼
                              Tableau de bord Streamlit
```

Le traitement est exclusivement par lots : le jeu de données est un fichier historique, chargé directement dans HDFS. Une collecte continue avec un traitement en temps réel serait une extension naturelle si les avis arrivaient en flux.

### Technologies et justification

| Technologie | Rôle | Justification |
|---|---|---|
| HDFS | Stockage | Fichier découpé en blocs de 128 Mo et répliqué ; le traitement s'exécute au plus près des données |
| MapReduce (Hadoop Streaming) | Word count avec et sans combiner | Modèle de référence pour les patterns de récapitulation ; le combiner réduit fortement le volume du shuffle |
| Spark | Nettoyage, analyses, index inversé | Étapes enchaînées en mémoire, code court, DataFrames |
| Spark MLlib | Classification du sentiment | Entraînement itératif sur des données conservées en mémoire |
| MongoDB | Stockage final et requêtes | Un avis est un document JSON semi-structuré ; recherche rapide par produit et par mot |
| Streamlit | Visualisation | Interface web légère qui lit directement la base MongoDB |

### Environnement

Image Docker `liliasfaxi/spark-hadoop:hv-2.7.2` (Hadoop 2.7.2, Spark 2.2.0, Python 3.5.2), avec un nœud maître et deux nœuds esclaves, plus un conteneur `mongo:7`. Le tableau de bord s'exécute sur la machine hôte avec un Python récent (3.9 ou plus).

## Fonctionnement du pipeline

| Étape | Script | Rôle | Sortie |
|---|---|---|---|
| 1. Chargement | commande `hdfs dfs -put` | Dépose `Reviews.csv` dans HDFS | `/projet/reviews/raw` |
| 2. Nettoyage | `01_clean.py` | Passe le texte en minuscules, retire le HTML et les caractères non alphabétiques, supprime les textes vides et les doublons, crée l'étiquette (4 et 5 étoiles : positif ; 1 et 2 : négatif ; 3 : exclu) | `clean` (Parquet), `clean_tsv` (texte tabulé) |
| 3. Contrôle | `00_count.py`, `01c_stats.py` | Compte les avis avant et après nettoyage, répartition par note | affichage |
| 4. Compactage | `01b_compact.py` | Regroupe les 200 petits fichiers en 4 pour éviter 200 tâches map | `clean_tsv4` |
| 5. MapReduce | `mapreduce/mapper.py`, `reducer.py` | Word count avec mots vides filtrés ; le reducer sert aussi de combiner | `wc_mr_combiner`, `wc_mr_simple` |
| 6. Analyses Spark | `02_analysis.py` | Word count et top 10 des produits (au moins 50 avis) | affichage |
| 7. Index inversé | `03_inverted_index.py` | Pour chaque mot présent dans au moins 5 avis : nombre d'avis et 100 identifiants | `inverted_index` (JSON) |
| 8. Modèle | `04_ml.py` | TF-IDF (10 000 mots, présents dans au moins 5 avis) puis régression logistique à classes pondérées, découpe 80/20 | `predictions` (Parquet) |
| 9. Export | `05_export.py` | Joint les avis nettoyés et les prédictions, écrit du JSON | `export_json` |
| 10. Stockage final | `mongoimport` | Charge les deux collections dans MongoDB | base `avis` |
| 11. Visualisation | `dashboard/app.py` | Affiche les résultats depuis MongoDB | page web locale |

### Le modèle de sentiment

Le texte est découpé en mots, les mots vides sont retirés, puis chaque avis est représenté par un vecteur TF-IDF. Une régression logistique apprend à séparer avis positifs et négatifs. Comme environ 84 % des avis étiquetés sont positifs, chaque classe reçoit un poids inversement proportionnel à sa fréquence, et l'évaluation s'appuie sur l'AUC et sur la précision et le rappel de la classe négative plutôt que sur l'exactitude seule. Les avis à 3 étoiles, ambigus, sont exclus de l'apprentissage et n'ont donc pas de prédiction.

## Résultats

### Stockage HDFS

- 300 904 694 octets répartis en 3 blocs (128 Mo, 128 Mo et environ 31 Mo).
- Réplication de 2, car le cluster compte 2 DataNodes. Statut `HEALTHY`.

### Nettoyage

- 568 454 avis avant, 567 118 après.
- Répartition des notes : 1 étoile 51 957, 2 étoiles 29 751, 3 étoiles 42 550, 4 étoiles 80 535, 5 étoiles 362 325.
- Étiquettes : positif 442 860, négatif 81 708, exclus 42 550.

### MapReduce : effet du combiner

| | Sans combiner | Avec combiner |
|---|---|---|
| Couples émis par les mappers | 23 442 332 | 23 442 332 |
| Couples reçus par les reducers | 23 442 332 | 389 901 |
| Temps total de la commande | 35,6 s | 21,5 s |

Les deux jobs produisent exactement le même résultat. Les temps proviennent d'une seule exécution chacun.

### Spark

- Même Top 20 de mots que MapReduce : good 199 658, taste 172 302, great 166 508, coffee 165 802, product 150 854.
- Word count seul : environ 10 s, hors démarrage de l'application.
- Commande complète sur YARN, avec en plus le top 10 des produits : environ 24 s.
- À cette taille de données, MapReduce avec combiner et Spark ont des temps comparables.

### Index inversé

- 41 569 mots indexés.
- Exemple : le mot `delivery` apparaît dans 7 861 avis.

### Modèle de sentiment

| | 10 % des données | 100 % des données |
|---|---|---|
| Avis utilisés | 52 599 | 524 568 |
| Temps d'entraînement | 4,4 s | 21,4 s |
| AUC | 0,891 | 0,954 |
| Précision (classe négative) | 0,639 | 0,649 |
| Rappel (classe négative) | 0,705 | 0,887 |

Matrice de confusion sur l'échantillon de test (104 602 avis, apprentissage sur 100 % des données) :

| | Prédit négatif | Prédit positif |
|---|---|---|
| Vraiment négatif | 14 527 | 1 847 |
| Vraiment positif | 7 843 | 80 385 |

Une nouvelle exécution donne des valeurs très proches (AUC 0,9542, rappel 0,886, précision 0,65) : de légères variations sont normales d'une exécution à l'autre.

### MongoDB

- 567 118 documents dans la collection `reviews`, 41 569 dans `inverted`.
- Prédictions : 112 288 négatifs, 412 280 positifs, 42 550 sans prédiction.
- Exemple : le produit `B000NMJWZO` compte 542 avis, avec une moyenne de 4,88.

## Tableau de bord

Une application Streamlit lit la base MongoDB et propose quatre onglets :

| Onglet | Contenu |
|---|---|
| Vue d'ensemble | Nombre d'avis et de mots, répartition des notes, sentiment prédit |
| Mots | Mots présents dans le plus d'avis, recherche par mot |
| Produits | Produits les mieux notés, recherche par produit et avis négatifs détectés |
| Modèle | AUC, rappel, précision et matrice de confusion |

Installation et lancement, depuis la racine du dépôt :

```
pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py
```

L'application se connecte par défaut à `mongodb://127.0.0.1:27018`. Une autre adresse peut être indiquée avec la variable d'environnement `MONGO_URI`. Le tableau de bord s'ouvre à l'adresse `http://localhost:8501`.

## Limites et perspectives

- Le jeu de données est modeste pour du Big Data (environ 300 Mo). L'architecture est conçue pour passer à l'échelle, mais l'avantage de Spark sur MapReduce n'est pas mesurable à cette taille.
- Les temps proviennent d'une exécution unique sur un petit cluster.
- Le modèle est un « sac de mots » : il ne tient compte ni de la négation ni de l'ironie. La pondération des classes donne un bon rappel au prix d'une précision de 0,65.
- La liste de mots vides du mapper est écrite à la main et laisse passer quelques mots peu informatifs.
- MongoDB fonctionne sans authentification, ce qui convient à une démonstration locale mais pas à un déploiement.
- Pistes : ajouter le titre de l'avis au texte, régler le seuil de décision, ajouter des bigrammes, et mettre en place une collecte continue avec traitement en temps réel.

## Structure du dépôt

```
README.md
scripts/      00_count.py  01_clean.py  01b_compact.py  01c_stats.py
              02_analysis.py  03_inverted_index.py  04_ml.py  05_export.py
mapreduce/    mapper.py  reducer.py
mongo/        queries.js
demo/         demo.sh  04_ml_demo.py
dashboard/    app.py  requirements.txt
docs/         log_simple.txt  log_combiner.txt
```

## Exécution

### Préparer l'environnement

Depuis la racine du dépôt, sur la machine qui héberge les conteneurs :

```
docker exec hadoop-master mkdir -p /root/projet
docker cp scripts/. hadoop-master:/root/projet/
docker cp mapreduce hadoop-master:/root/projet/
docker cp demo/. hadoop-master:/root/projet/
docker exec hadoop-master chmod +x /root/projet/demo.sh /root/projet/mapreduce/mapper.py /root/projet/mapreduce/reducer.py
docker cp Reviews.csv hadoop-master:/root/Reviews.csv
```

Dans le conteneur `hadoop-master` :

```
cd /root
./start-hadoop.sh
apt-get update
apt-get install -y python3-numpy
```

Le module `numpy` est nécessaire à Spark MLlib.

### Charger les données dans HDFS

```
hdfs dfs -mkdir -p /projet/reviews/raw
hdfs dfs -put /root/Reviews.csv /projet/reviews/raw/
hdfs fsck /projet/reviews/raw/Reviews.csv -files -blocks
```

### Nettoyer et préparer

```
cd /root/projet
export PYSPARK_PYTHON=python3
spark-submit --master local[2] 01_clean.py
spark-submit --master local[2] 01b_compact.py
spark-submit --master local[2] 00_count.py
spark-submit --master local[2] 01c_stats.py
```

### Lancer le word count MapReduce

```
cd /root/projet/mapreduce
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar -input /projet/reviews/clean_tsv4 -output /projet/reviews/wc_mr_combiner -mapper mapper.py -combiner reducer.py -reducer reducer.py -file mapper.py -file reducer.py
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar -input /projet/reviews/clean_tsv4 -output /projet/reviews/wc_mr_simple -mapper mapper.py -reducer reducer.py -file mapper.py -file reducer.py
hdfs dfs -cat /projet/reviews/wc_mr_combiner/part-* | sort -t$'\t' -k2 -nr | head -20
```

Hadoop refuse d'écrire dans un dossier de sortie existant : supprimer le dossier avec `hdfs dfs -rm -r` avant de relancer un job.

### Analyser, indexer, entraîner et exporter

```
cd /root/projet
spark-submit --master yarn --deploy-mode client 02_analysis.py
spark-submit --master yarn --deploy-mode client 03_inverted_index.py
spark-submit --master local[2] --driver-memory 2g 04_ml.py
spark-submit --master local[2] --driver-memory 2g 05_export.py
hdfs dfs -getmerge /projet/reviews/export_json /root/reviews.json
hdfs dfs -getmerge /projet/reviews/inverted_index /root/inverted.json
```

Le script `04_ml.py` accepte une fraction des données en argument (par exemple `0.1`) pour un essai rapide.

### Charger MongoDB et interroger

Sur la machine hôte :

```
docker run -d --name mongo-avis -p 27018:27017 mongo:7
docker cp hadoop-master:/root/reviews.json reviews.json
docker cp hadoop-master:/root/inverted.json inverted.json
docker cp reviews.json mongo-avis:/tmp/reviews.json
docker cp inverted.json mongo-avis:/tmp/inverted.json
docker exec mongo-avis mongoimport --db avis --collection reviews --file /tmp/reviews.json
docker exec mongo-avis mongoimport --db avis --collection inverted --file /tmp/inverted.json
docker exec -i mongo-avis mongosh --quiet < mongo/queries.js
```

Le fichier `mongo/queries.js` crée les index et lance les requêtes de démonstration : nombre de documents, recherche par mot, statistiques d'un produit, répartition des prédictions, avis négatifs prédits.

Si un autre MongoDB occupe déjà le port choisi sur la machine hôte, publier le conteneur sur un autre port (par exemple `-p 27019:27017`) et l'indiquer au tableau de bord avec `MONGO_URI`.

## Démonstration

Le script `demo/demo.sh` enchaîne les étapes du pipeline devant un public : stockage HDFS, nettoyage, MapReduce avec combiner, Spark, index inversé et modèle. Il s'arrête après chaque scène et n'écrit que dans `/projet/demo`, sans toucher aux résultats existants.

```
cd /root/projet
./demo.sh
```

La partie MongoDB se déroule ensuite dans un second terminal :

```
docker exec -it mongo-avis mongosh avis --quiet
```
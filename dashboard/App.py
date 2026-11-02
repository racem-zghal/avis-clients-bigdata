import os

import pandas as pd
import streamlit as st
from pymongo import MongoClient

st.set_page_config(page_title="Avis clients - Big Data", layout="wide")

LABELS = {0: "Négatif prédit", 1: "Positif prédit"}


@st.cache_resource
def get_db():
    uri = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27018")
    client = MongoClient(uri, serverSelectionTimeoutMS=3000)
    client.admin.command("ping")
    return client["avis"]


try:
    db = get_db()
except Exception:
    st.error("MongoDB est inaccessible. Vérifiez que le conteneur mongo-avis2 est démarré sur le port 27018.")
    st.stop()

if db.reviews.estimated_document_count() == 0:
    st.error("La base « avis » est vide sur le MongoDB joint par l’application.")
    st.write("Bases visibles sur ce serveur :", db.client.list_database_names())
    st.stop()


@st.cache_data(show_spinner=False)
def totals():
    return db.reviews.estimated_document_count(), db.inverted.estimated_document_count()


@st.cache_data(show_spinner=False)
def score_distribution():
    rows = list(db.reviews.aggregate([
        {"$group": {"_id": "$score", "avis": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]))
    df = pd.DataFrame(rows).rename(columns={"_id": "note"})
    df["note"] = df["note"].astype(str) + " étoile(s)"
    return df.set_index("note")


@st.cache_data(show_spinner=False)
def sentiment_distribution():
    rows = list(db.reviews.aggregate([
        {"$group": {"_id": "$sentiment_pred", "avis": {"$sum": 1}}},
    ]))
    df = pd.DataFrame(rows)
    df["sentiment"] = df["_id"].map(lambda x: LABELS.get(x, "Sans prédiction (3 étoiles)"))
    return df[["sentiment", "avis"]].set_index("sentiment")


@st.cache_data(show_spinner=False)
def top_words(n):
    rows = list(db.inverted.find({}, {"_id": 0, "word": 1, "n_reviews": 1}).sort("n_reviews", -1).limit(n))
    return pd.DataFrame(rows).set_index("word")


@st.cache_data(show_spinner=True)
def top_products(min_reviews):
    pipeline = [
        {"$group": {"_id": "$ProductId", "avis": {"$sum": 1}, "note_moyenne": {"$avg": "$score"}}},
        {"$match": {"avis": {"$gte": min_reviews}}},
        {"$sort": {"note_moyenne": -1}},
        {"$limit": 10},
    ]
    df = pd.DataFrame(list(db.reviews.aggregate(pipeline, allowDiskUse=True)))
    return df.rename(columns={"_id": "produit"}).set_index("produit")


def product_stats(product_id):
    rows = list(db.reviews.aggregate([
        {"$match": {"ProductId": product_id}},
        {"$group": {"_id": None, "avis": {"$sum": 1}, "note_moyenne": {"$avg": "$score"}}},
    ]))
    return rows[0] if rows else None


def fmt(n):
    return f"{n:,}".replace(",", " ")


st.title("Analyse d’avis clients")
st.caption("Pipeline Big Data : HDFS, MapReduce, Spark, MLlib, MongoDB")

n_reviews, n_words = totals()
c1, c2 = st.columns(2)
c1.metric("Avis stockés", fmt(n_reviews))
c2.metric("Mots indexés", fmt(n_words))

tab_overview, tab_words, tab_products, tab_model = st.tabs(
    ["Vue d’ensemble", "Mots", "Produits", "Modèle"]
)

with tab_overview:
    left, right = st.columns(2)
    with left:
        st.subheader("Répartition des notes")
        st.bar_chart(score_distribution())
    with right:
        st.subheader("Sentiment prédit")
        st.bar_chart(sentiment_distribution())

with tab_words:
    st.subheader("Mots présents dans le plus d’avis")
    n = st.slider("Nombre de mots", 10, 50, 20)
    st.bar_chart(top_words(n))
    st.subheader("Recherche par mot")
    word = st.text_input("Mot à chercher", value="delivery")
    if word.strip():
        doc = db.inverted.find_one({"word": word.strip().lower()}, {"_id": 0})
        if doc is None:
            st.warning("Ce mot n’est pas dans l’index inversé.")
        else:
            st.write(f"{fmt(doc['n_reviews'])} avis contiennent ce mot.")
            rows = list(db.reviews.find(
                {"Id": {"$in": doc["review_ids"][:10]}},
                {"_id": 0, "score": 1, "summary": 1},
            ))
            st.dataframe(pd.DataFrame(rows), use_container_width=True)

with tab_products:
    st.subheader("Produits les mieux notés")
    min_reviews = st.slider("Nombre minimal d’avis", 10, 200, 50, step=10)
    st.dataframe(top_products(min_reviews), use_container_width=True)
    st.subheader("Recherche par produit")
    product_id = st.text_input("Identifiant produit", value="B000NMJWZO")
    if product_id.strip():
        stats = product_stats(product_id.strip())
        if stats is None:
            st.warning("Produit introuvable.")
        else:
            a, b = st.columns(2)
            a.metric("Avis", fmt(stats["avis"]))
            b.metric("Note moyenne", f"{stats['note_moyenne']:.2f}".replace(".", ","))
            negatives = list(db.reviews.find(
                {"ProductId": product_id.strip(), "sentiment_pred": 0, "score": {"$lte": 2}},
                {"_id": 0, "score": 1, "summary": 1},
            ).limit(5))
            st.write("Avis négatifs détectés par le modèle (jusqu’à 5) :")
            st.dataframe(pd.DataFrame(negatives), use_container_width=True)

with tab_model:
    st.subheader("Performances du modèle")
    st.caption("Valeurs mesurées lors de l’entraînement sur 524 568 avis étiquetés ; test sur 104 602 avis.")
    m1, m2, m3 = st.columns(3)
    m1.metric("AUC", "0,954")
    m2.metric("Rappel, avis négatifs", "0,89")
    m3.metric("Précision, avis négatifs", "0,65")
    st.write("Matrice de confusion")
    st.dataframe(
        pd.DataFrame(
            [[14527, 1847], [7843, 80385]],
            index=["Négatif réel", "Positif réel"],
            columns=["Prédit négatif", "Prédit positif"],
        ),
        use_container_width=True,
    )
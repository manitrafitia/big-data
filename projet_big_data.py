# -*- coding: utf-8 -*-
# Projet Big Data - Backend Flask local

!pip install pyspark flask seaborn matplotlib

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as _sum
from flask import Flask, jsonify
import threading

# ----------------------------
# 1. Initialisation Spark
# ----------------------------
spark = SparkSession.builder.appName("ProductionAnalysis").getOrCreate()

# ----------------------------
# 2. Chargement du dataset CSV
# ----------------------------
# Pour Colab, uploader le fichier CSV
from google.colab import files
uploaded = files.upload()

# Suppose le fichier s'appelle production.csv
df = spark.read.csv("production.csv", header=True, inferSchema=True)
df.show(5)

# ----------------------------
# 3. Nettoyage et transformation
# ----------------------------
df = df.withColumn("Quantité_produite", col("Quantité_produite").cast("int"))
df = df.withColumn("Défauts", col("Défauts").cast("int"))
df = df.withColumn("Temps_arret(min)", col("Temps_arret(min)").cast("int"))

# ----------------------------
# 4. Calcul des KPIs
# ----------------------------
prod_machine = df.groupBy("Machine").agg(
    _sum("Quantité_produite").alias("Total_production"),
    avg("Défauts").alias("Defauts_moyens"),
    avg("Temps_arret(min)").alias("Temps_arret_moyen")
)
prod_machine.show()

# Conversion en Pandas pour visualisation ou Flask
pdf = prod_machine.toPandas()

# ----------------------------
# 5. Visualisations (optionnel)
# ----------------------------
plt.figure(figsize=(8,5))
sns.barplot(x="Machine", y="Total_production", data=pdf)
plt.title("Production totale par machine")
plt.show()

plt.figure(figsize=(8,5))
sns.barplot(x="Machine", y="Defauts_moyens", data=pdf)
plt.title("Défauts moyens par machine")
plt.show()

plt.figure(figsize=(8,5))
sns.barplot(x="Machine", y="Temps_arret_moyen", data=pdf)
plt.title("Temps d'arrêt moyen par machine")
plt.show()

# ----------------------------
# 6. Backend Flask local
# ----------------------------
app = Flask(__name__)

@app.route("/kpi", methods=["GET"])
def get_kpi():
    return jsonify(pdf.to_dict(orient="records"))

# Lancer Flask sur localhost
def run_app():
    app.run(host="0.0.0.0", port=5000)

thread = threading.Thread(target=run_app)
thread.start()

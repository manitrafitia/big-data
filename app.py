# -*- coding: utf-8 -*-
from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as _sum, max as _max, min as _min, count, when
from pyspark.sql.types import *
import json
from datetime import datetime, timedelta
import random

# ----------------------------
# 1. Initialisation Spark
# ----------------------------
spark = SparkSession.builder \
    .appName("ProductionAnalysisAdvanced") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# ----------------------------
# 2. Génération de données de test (si le CSV n'existe pas)
# ----------------------------
def generate_sample_data():
    """Génère des données de test pour la démonstration"""
    machines = ['A001', 'A002', 'A003', 'B001', 'B002', 'C001', 'C002']
    shifts = ['Matin', 'Après-midi', 'Nuit']
    operators = ['Jean', 'Marie', 'Pierre', 'Sophie', 'Luc', 'Anna', 'Thomas']
    
    data = []
    for i in range(1000):  # 1000 enregistrements
        date = datetime.now() - timedelta(days=random.randint(0, 30))
        data.append({
            'Date': date.strftime('%Y-%m-%d'),
            'Machine': random.choice(machines),
            'Shift': random.choice(shifts),
            'Operateur': random.choice(operators),
            'Quantité_produite': random.randint(800, 1200),
            'Défauts': random.randint(0, 50),
            'Temps_arret(min)': random.randint(0, 120),
            'Maintenance_preventive': random.choice([True, False]),
            'Temperature': round(random.uniform(18.0, 35.0), 1),
            'Humidite': round(random.uniform(40.0, 80.0), 1)
        })
    
    df = pd.DataFrame(data)
    df.to_csv('production_enhanced.csv', index=False)
    return df

# ----------------------------
# 3. Chargement ou génération des données
# ----------------------------
try:
    df = spark.read.csv("production_enhanced.csv", header=True, inferSchema=True)
except:
    print("Fichier CSV non trouvé. Génération de données de test...")
    sample_df = generate_sample_data()
    df = spark.createDataFrame(sample_df)

# ----------------------------
# 4. Nettoyage et transformation des données
# ----------------------------
df = df.withColumn("Quantité_produite", col("Quantité_produite").cast("int")) \
     .withColumn("Défauts", col("Défauts").cast("int")) \
     .withColumn("Temps_arret(min)", col("Temps_arret(min)").cast("int")) \
     .withColumn("Temperature", col("Temperature").cast("double")) \
     .withColumn("Humidite", col("Humidite").cast("double"))

# Calcul de métriques dérivées
df = df.withColumn("Taux_defaut", (col("Défauts") / col("Quantité_produite")) * 100) \
     .withColumn("Efficacite", when(col("Temps_arret(min)") == 0, 100.0)
                 .otherwise(100.0 - (col("Temps_arret(min)") / 8.0)))  # Efficacité sur 8h de shift

# ----------------------------
# 5. Calculs des KPIs avancés
# ----------------------------

# KPIs par machine
prod_machine = df.groupBy("Machine").agg(
    _sum("Quantité_produite").alias("Total_production"),
    avg("Défauts").alias("Defauts_moyens"),
    avg("Temps_arret(min)").alias("Temps_arret_moyen"),
    avg("Taux_defaut").alias("Taux_defaut_moyen"),
    avg("Efficacite").alias("Efficacite_moyenne"),
    count("*").alias("Nombre_cycles"),
    avg("Temperature").alias("Temperature_moyenne"),
    avg("Humidite").alias("Humidite_moyenne")
)

# KPIs par shift
prod_shift = df.groupBy("Shift").agg(
    _sum("Quantité_produite").alias("Total_production"),
    avg("Défauts").alias("Defauts_moyens"),
    avg("Temps_arret(min)").alias("Temps_arret_moyen"),
    avg("Efficacite").alias("Efficacite_moyenne")
)

# KPIs par opérateur
prod_operator = df.groupBy("Operateur").agg(
    _sum("Quantité_produite").alias("Total_production"),
    avg("Défauts").alias("Defauts_moyens"),
    avg("Efficacite").alias("Efficacite_moyenne"),
    count("*").alias("Shifts_travailles")
)

# Tendances temporelles (par date)
prod_trends = df.groupBy("Date").agg(
    _sum("Quantité_produite").alias("Production_journaliere"),
    avg("Défauts").alias("Defauts_moyens"),
    avg("Efficacite").alias("Efficacite_journaliere")
).orderBy("Date")

# Alertes et anomalies
alertes = df.filter(
    (col("Taux_defaut") > 5) | 
    (col("Temps_arret(min)") > 60) | 
    (col("Efficacite") < 80)
).select("Date", "Machine", "Shift", "Operateur", "Taux_defaut", "Temps_arret(min)", "Efficacite")

# Conversion en Pandas pour l'API
pdf_machine = prod_machine.toPandas()
pdf_shift = prod_shift.toPandas()
pdf_operator = prod_operator.toPandas()
pdf_trends = prod_trends.toPandas()
pdf_alertes = alertes.toPandas()

# Statistiques globales
stats_globales = {
    "production_totale": int(df.agg(_sum("Quantité_produite")).collect()[0][0]),
    "defauts_totaux": int(df.agg(_sum("Défauts")).collect()[0][0]),
    "temps_arret_total": int(df.agg(_sum("Temps_arret(min)")).collect()[0][0]),
    "efficacite_globale": float(df.agg(avg("Efficacite")).collect()[0][0]),
    "nombre_machines": df.select("Machine").distinct().count(),
    "nombre_operateurs": df.select("Operateur").distinct().count(),
    "cycles_totaux": df.count()
}

# ----------------------------
# 6. Backend Flask avec API étendue
# ----------------------------
app = Flask(__name__)
CORS(app)

@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "message": "API Dashboard Production Avancé",
        "endpoints": [
            "/kpi/machines",
            "/kpi/shifts", 
            "/kpi/operators",
            "/kpi/trends",
            "/kpi/alerts",
            "/kpi/global",
            "/machines",
            "/health"
        ]
    })

@app.route("/kpi/machines", methods=["GET"])
def get_kpi_machines():
    return jsonify(pdf_machine.to_dict(orient="records"))

@app.route("/kpi/shifts", methods=["GET"])
def get_kpi_shifts():
    return jsonify(pdf_shift.to_dict(orient="records"))

@app.route("/kpi/operators", methods=["GET"])
def get_kpi_operators():
    return jsonify(pdf_operator.to_dict(orient="records"))

@app.route("/kpi/trends", methods=["GET"])
def get_trends():
    return jsonify(pdf_trends.to_dict(orient="records"))

@app.route("/kpi/alerts", methods=["GET"])
def get_alerts():
    return jsonify(pdf_alertes.to_dict(orient="records"))

@app.route("/kpi/global", methods=["GET"])
def get_global_stats():
    return jsonify(stats_globales)

@app.route("/machines", methods=["GET"])
def get_machines_list():
    machines = df.select("Machine").distinct().collect()
    return jsonify([row.Machine for row in machines])

@app.route("/machine/<machine_id>/details", methods=["GET"])
def get_machine_details(machine_id):
    machine_data = df.filter(col("Machine") == machine_id)
    if machine_data.count() == 0:
        return jsonify({"error": "Machine non trouvée"}), 404
    
    details = machine_data.agg(
        _sum("Quantité_produite").alias("production_totale"),
        avg("Défauts").alias("defauts_moyens"),
        avg("Temps_arret(min)").alias("temps_arret_moyen"),
        _max("Quantité_produite").alias("production_max"),
        _min("Quantité_produite").alias("production_min"),
        avg("Temperature").alias("temperature_moyenne"),
        avg("Humidite").alias("humidite_moyenne")
    ).collect()[0]
    
    return jsonify({
        "machine": machine_id,
        "production_totale": int(details.production_totale),
        "defauts_moyens": float(details.defauts_moyens),
        "temps_arret_moyen": float(details.temps_arret_moyen),
        "production_max": int(details.production_max),
        "production_min": int(details.production_min),
        "temperature_moyenne": float(details.temperature_moyenne),
        "humidite_moyenne": float(details.humidite_moyenne)
    })

@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "spark_status": "active",
        "data_records": df.count()
    })

# Endpoint pour la recherche et filtrage
@app.route("/search", methods=["GET"])
def search_data():
    machine = request.args.get('machine')
    shift = request.args.get('shift')
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    
    filtered_df = df
    
    if machine:
        filtered_df = filtered_df.filter(col("Machine") == machine)
    if shift:
        filtered_df = filtered_df.filter(col("Shift") == shift)
    if date_from:
        filtered_df = filtered_df.filter(col("Date") >= date_from)
    if date_to:
        filtered_df = filtered_df.filter(col("Date") <= date_to)
    
    result = filtered_df.limit(100).toPandas()  # Limiter à 100 résultats
    return jsonify(result.to_dict(orient="records"))

# ----------------------------
# 7. Lancement du serveur Flask
# ----------------------------
if __name__ == "__main__":
    print("🚀 Démarrage du serveur Flask avec API avancée...")
    print("📊 Dashboard disponible sur http://localhost:5000")
    print("📡 API endpoints disponibles sur http://localhost:5000/")
    app.run(host="0.0.0.0", port=5000, debug=True)
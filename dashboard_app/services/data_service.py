import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import random
from datetime import datetime, timedelta

spark = SparkSession.builder \
    .appName("ProductionAnalysisAdvanced") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

def generate_sample_data():
    machines = ['A001', 'A002', 'A003', 'B001', 'B002', 'C001']
    shifts = ['Matin', 'Après-midi', 'Nuit']
    operators = ['Jean', 'Marie', 'Pierre', 'Sophie', 'Luc', 'Anna']

    data = []
    for _ in range(1000):
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
    df.to_csv("data/production_enhanced.csv", index=False)
    return df

try:
    df = spark.read.csv("data/production_enhanced.csv", header=True, inferSchema=True)
except:
    print("⚠️ CSV introuvable, génération de données fictives...")
    sample_df = generate_sample_data()
    df = spark.createDataFrame(sample_df)

df = df.withColumn("Quantité_produite", col("Quantité_produite").cast("int")) \
       .withColumn("Défauts", col("Défauts").cast("int")) \
       .withColumn("Temps_arret(min)", col("Temps_arret(min)").cast("int")) \
       .withColumn("Temperature", col("Temperature").cast("double")) \
       .withColumn("Humidite", col("Humidite").cast("double")) \
       .withColumn("Taux_defaut", (col("Défauts") / col("Quantité_produite")) * 100) \
       .withColumn("Efficacite", when(col("Temps_arret(min)") == 0, 100.0)
                   .otherwise(100.0 - (col("Temps_arret(min)") / 8.0)))

def get_dataframe():
    return df

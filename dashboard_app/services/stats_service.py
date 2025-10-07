from pyspark.sql.functions import avg, sum as _sum, count
from ..services.data_service import get_dataframe

df = get_dataframe()

def get_kpi_machines():
    pdf = df.groupBy("Machine").agg(
        _sum("Quantité_produite").alias("Total_production"),
        avg("Défauts").alias("Defauts_moyens"),
        avg("Temps_arret(min)").alias("Temps_arret_moyen"),
        avg("Taux_defaut").alias("Taux_defaut_moyen"),
        avg("Efficacite").alias("Efficacite_moyenne")
    ).toPandas()
    return pdf.to_dict(orient="records")

def get_kpi_shifts():
    pdf = df.groupBy("Shift").agg(
        _sum("Quantité_produite").alias("Total_production"),
        avg("Défauts").alias("Defauts_moyens"),
        avg("Efficacite").alias("Efficacite_moyenne")
    ).toPandas()
    return pdf.to_dict(orient="records")

def get_kpi_operators():
    pdf = df.groupBy("Operateur").agg(
        _sum("Quantité_produite").alias("Total_production"),
        avg("Défauts").alias("Defauts_moyens"),
        avg("Efficacite").alias("Efficacite_moyenne"),
        count("*").alias("Shifts_travailles")
    ).toPandas()
    return pdf.to_dict(orient="records")

def get_trends():
    pdf = df.groupBy("Date").agg(
        _sum("Quantité_produite").alias("Production_journaliere"),
        avg("Défauts").alias("Defauts_moyens"),
        avg("Efficacite").alias("Efficacite_journaliere")
    ).orderBy("Date").toPandas()
    return pdf.to_dict(orient="records")

def get_alerts():
    pdf = df.filter(
        (df["Taux_defaut"] > 5) | 
        (df["Temps_arret(min)"] > 60) | 
        (df["Efficacite"] < 80)
    ).select("Date", "Machine", "Shift", "Operateur", "Taux_defaut", "Temps_arret(min)", "Efficacite") \
     .toPandas()
    return pdf.to_dict(orient="records")

def get_global_stats():
    stats = {
        "production_totale": int(df.agg(_sum("Quantité_produite")).collect()[0][0]),
        "defauts_totaux": int(df.agg(_sum("Défauts")).collect()[0][0]),
        "efficacite_globale": float(df.agg(avg("Efficacite")).collect()[0][0]),
        "nombre_machines": df.select("Machine").distinct().count(),
        "nombre_operateurs": df.select("Operateur").distinct().count(),
        "cycles_totaux": df.count()
    }
    return stats

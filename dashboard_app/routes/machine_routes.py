from flask import Blueprint, jsonify
from ..services.data_service import get_dataframe
from pyspark.sql.functions import avg, sum as _sum, min as _min, max as _max, col

machine_bp = Blueprint("machine", __name__, url_prefix="/machines")
df = get_dataframe()

@machine_bp.route("/", methods=["GET"])
def get_machines_list():
    machines = df.select("Machine").distinct().collect()
    return jsonify([row.Machine for row in machines])

@machine_bp.route("/<machine_id>/details", methods=["GET"])
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

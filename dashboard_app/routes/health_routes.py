from flask import Blueprint, jsonify
from ..services.data_service import get_dataframe
from datetime import datetime

health_bp = Blueprint("health", __name__, url_prefix="/health")
df = get_dataframe()

@health_bp.route("/", methods=["GET"])
def health_check():
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "spark_status": "active",
        "data_records": df.count()
    })

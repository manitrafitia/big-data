from flask import Blueprint, jsonify, request
from ..services.data_service import get_dataframe
from pyspark.sql.functions import col

search_bp = Blueprint("search", __name__, url_prefix="/search")
df = get_dataframe()

@search_bp.route("/", methods=["GET"])
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

    result = filtered_df.limit(100).toPandas()
    return jsonify(result.to_dict(orient="records"))

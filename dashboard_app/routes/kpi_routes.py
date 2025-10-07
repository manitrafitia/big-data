from flask import Blueprint, jsonify
from ..services import stats_service

kpi_bp = Blueprint("kpi", __name__, url_prefix="/kpi")

@kpi_bp.route("/machines", methods=["GET"])
def get_kpi_machines():
    return jsonify(stats_service.get_kpi_machines())

@kpi_bp.route("/shifts", methods=["GET"])
def get_kpi_shifts():
    return jsonify(stats_service.get_kpi_shifts())

@kpi_bp.route("/operators", methods=["GET"])
def get_kpi_operators():
    return jsonify(stats_service.get_kpi_operators())

@kpi_bp.route("/trends", methods=["GET"])
def get_trends():
    return jsonify(stats_service.get_trends())

@kpi_bp.route("/alerts", methods=["GET"])
def get_alerts():
    return jsonify(stats_service.get_alerts())

@kpi_bp.route("/global", methods=["GET"])
def get_global_stats():
    return jsonify(stats_service.get_global_stats())

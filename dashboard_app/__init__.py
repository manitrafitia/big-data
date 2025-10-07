from flask import Flask, jsonify
from flask_cors import CORS
from dashboard_app.routes import register_routes

def create_app():
    app = Flask(__name__)
    CORS(app)

    # Enregistrer les routes
    register_routes(app)

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
                "/health",
                "/search"
            ]
        })

    return app

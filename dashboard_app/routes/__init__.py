from .kpi_routes import kpi_bp
from .machine_routes import machine_bp
from .search_routes import search_bp
from .health_routes import health_bp

def register_routes(app):
    app.register_blueprint(kpi_bp)
    app.register_blueprint(machine_bp)
    app.register_blueprint(search_bp)
    app.register_blueprint(health_bp)

import datetime
from flask import Flask, current_app
from flask_mail import Mail
from config import Config
from tasks.scheduler import register_scheduler
from appdata_init import init_appdata

# ---- Import Blueprints (routes) ----
from routes.admin import bp as admin_bp
from routes.dashboard import bp as dashboard_bp
from routes.reports import bp as reports_bp
from routes.students import bp as student_bp
from routes.auth import bp as auth_bp
from routes.hod_dashboard import bp as hod_dashboard_bp
from routes.teacher_dashboard import bp as teacher_dashboard_bp
from routes.password_reset import bp as password_reset_bp

# ✅ Global Flask-Mail instance
mail = Mail()


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    # ✅ Initialize local SQLite (users/mappings/settings)
    init_appdata()

    # ✅ Configure default sender (fixes “no sender” error)
    if not app.config.get("MAIL_DEFAULT_SENDER"):
        app.config["MAIL_DEFAULT_SENDER"] = (
            "Maktabat al-Jamea",
            app.config.get("MAIL_USERNAME"),
        )

    # ✅ Initialize Flask-Mail
    mail.init_app(app)

    # ✅ Register Blueprints
    app.register_blueprint(admin_bp)
    app.register_blueprint(auth_bp, url_prefix="/")
    app.register_blueprint(dashboard_bp, url_prefix="/dashboard")
    app.register_blueprint(hod_dashboard_bp, url_prefix="/hod")
    app.register_blueprint(teacher_dashboard_bp, url_prefix="/teacher")
    app.register_blueprint(reports_bp, url_prefix="/reports")
    app.register_blueprint(student_bp, url_prefix="/students")
    app.register_blueprint(password_reset_bp)

    # ✅ Template helpers
    @app.context_processor
    def utility_processor():
        return dict(zip=zip)

    @app.context_processor
    def inject_now():
        """Inject current UTC time into templates."""
        return {"now": datetime.datetime.now(datetime.timezone.utc)}

    @app.context_processor
    def inject_current_app():
        """Make current_app available inside Jinja templates (used in base.html)."""
        return dict(current_app=current_app)

    # ✅ Start the background scheduler
    register_scheduler(app, mail)

    return app


if __name__ == "__main__":
    app = create_app()
    app.secret_key = app.config["SECRET_KEY"]
    app.run(debug=True, host="0.0.0.0", port=5000)

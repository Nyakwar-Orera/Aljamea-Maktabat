import datetime
import os
import logging

from flask import Flask, current_app
from flask_mail import Mail

from config import Config
from tasks.scheduler import register_scheduler
from appdata_init import init_appdata

# ---- Import Blueprints ----
from routes.admin import bp as admin_bp
from routes.dashboard import bp as dashboard_bp
from routes.reports import bp as reports_bp
from routes.students import bp as student_bp
from routes.auth import bp as auth_bp
from routes.hod_dashboard import bp as hod_dashboard_bp
from routes.teacher_dashboard import bp as teacher_dashboard_bp
from routes.password_reset import bp as password_reset_bp
from routes.profile import bp as profile_bp

mail = Mail()


def create_app():
    # Basic logging – change to ERROR if you want almost no output
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    logging.getLogger("waitress.queue").setLevel(logging.ERROR)

    app = Flask(__name__)
    app.config.from_object(Config)

    # Ensure upload folder exists for profile pictures
    os.makedirs(app.config["PROFILE_UPLOAD_FOLDER"], exist_ok=True)

    # Initialize local SQLite schema
    init_appdata()

    # Ensure MAIL_DEFAULT_SENDER has proper value
    sender = app.config.get("MAIL_DEFAULT_SENDER")
    if not sender or (isinstance(sender, tuple) and not sender[1]):
        app.config["MAIL_DEFAULT_SENDER"] = (
            f"Maktabat al-Jamea <{app.config.get('MAIL_USERNAME')}>"
        )

    # Initialize Flask-Mail
    mail.init_app(app)

    # Mail config log (non-sensitive)
    app.logger.info("Mail configuration loaded:")
    app.logger.info("  MAIL_SERVER = %s", app.config.get("MAIL_SERVER"))
    app.logger.info("  MAIL_PORT = %s", app.config.get("MAIL_PORT"))
    app.logger.info("  MAIL_USE_TLS = %s", app.config.get("MAIL_USE_TLS"))
    app.logger.info("  MAIL_USERNAME = %s", app.config.get("MAIL_USERNAME"))
    app.logger.info(
        "  MAIL_DEFAULT_SENDER = %s",
        app.config.get("MAIL_DEFAULT_SENDER"),
    )

    # Helpers available in Jinja templates
    app.jinja_env.globals.update(
        zip=zip,
        enumerate=enumerate,
        len=len,
        list=list,
        sorted=sorted,
    )

    # -------------------------
    # Register blueprints
    # -------------------------

    # 🔐 Auth / login at root "/"
    app.register_blueprint(auth_bp)  # <-- no url_prefix here

    # ⚙️ Admin settings
    app.register_blueprint(admin_bp)

    # 📊 Dashboards
    app.register_blueprint(dashboard_bp, url_prefix="/dashboard")
    app.register_blueprint(hod_dashboard_bp, url_prefix="/hod")
    app.register_blueprint(teacher_dashboard_bp, url_prefix="/teacher")

    # 📈 Reports & students
    app.register_blueprint(reports_bp, url_prefix="/reports")
    app.register_blueprint(student_bp, url_prefix="/students")

    # 🔑 Password reset + profile
    app.register_blueprint(password_reset_bp)
    app.register_blueprint(profile_bp, url_prefix="/profile")

    # Context processors
    @app.context_processor
    def inject_now():
        return {"now": datetime.datetime.now(datetime.timezone.utc)}

    @app.context_processor
    def inject_current_app():
        return dict(current_app=current_app)

    # Start scheduler
    register_scheduler(app, mail)

    return app


if __name__ == "__main__":
    app = create_app()
    app.secret_key = app.config["SECRET_KEY"]

    use_waitress = os.getenv("USE_WAITRESS", "false").strip().lower() == "true"
    if use_waitress:
        from waitress import serve

        app.logger.info("Starting server with Waitress")
        serve(app, host="0.0.0.0", port=5000)
    else:
        app.logger.info("Starting server with Flask dev server")
        app.run(debug=False, host="0.0.0.0", port=5000)

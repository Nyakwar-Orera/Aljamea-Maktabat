import datetime
import os
import logging

from flask import Flask, current_app, g
from flask_mail import Mail
from flask_wtf.csrf import CSRFProtect

from config import Config
from tasks.scheduler import register_scheduler
from appdata_init import init_appdata
# from filters import register_filters

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
from routes.lab_usage import bp as lab_usage_bp
from routes.library import bp as library_bp

mail = Mail()
csrf = CSRFProtect()


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

    # Initialize CSRF Protection
    csrf.init_app(app)

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
    # Error Handlers
    # -------------------------
    from flask import render_template

    @app.errorhandler(404)
    def page_not_found(e):
        return render_template("errors/404.html"), 404

    @app.errorhandler(500)
    def internal_server_error(e):
        return render_template("errors/500.html"), 500

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
    app.register_blueprint(lab_usage_bp)
    app.register_blueprint(library_bp)

    # Health check endpoint
    @app.route("/health")
    def health():
        return {
            "status": "healthy",
            "version": "1.0.0",
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        }

    # Context processors
    @app.context_processor
    def inject_now():
        from services.koha_queries import get_ay_bounds
        
        start, end = get_ay_bounds()
        current_year = Config.CURRENT_ACADEMIC_YEAR()
        
        return {
            "now": datetime.datetime.now(datetime.timezone.utc),
            "current_academic_year": current_year,
            "ay_start": start,
            "ay_end": end,
            "ay_period": f"{start} to {end}" if start and end else "TBD"
        }

    @app.context_processor
    def inject_current_app():
        return dict(current_app=current_app)

    # Start scheduler
    register_scheduler(app, mail)

    # -------------------------
    # Security Headers
    # -------------------------
    @app.after_request
    def set_security_headers(response):
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "SAMEORIGIN"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        # Content Security Policy - allow Bootstrap, Google Fonts, and self
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://code.jquery.com https://cdn.datatables.net https://cdnjs.cloudflare.com; "
            "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://fonts.googleapis.com https://cdn.datatables.net; "
            "font-src 'self' https://fonts.gstatic.com https://cdn.jsdelivr.net; "
            "img-src 'self' data:; "
            "connect-src 'self';"
        )
        return response

    return app


if __name__ == "__main__":
    app = create_app()
    app.secret_key = app.config["SECRET_KEY"]

    use_waitress = os.getenv("USE_WAITRESS", "false").strip().lower() == "true"
    debug_mode = os.getenv("FLASK_DEBUG", "false").strip().lower() == "true" or app.config.get("DEBUG", False)
    
    if use_waitress:
        from waitress import serve
        app.logger.info("Starting server with Waitress")
        serve(app, host="0.0.0.0", port=5000)
    else:
        app.logger.info(f"Starting server with Flask dev server (debug={'on' if debug_mode else 'off'})")
        app.run(debug=debug_mode, host="0.0.0.0", port=5000)

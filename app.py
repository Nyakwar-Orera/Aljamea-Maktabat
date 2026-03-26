# app.py — PRODUCTION HARDENED
import datetime
import os
import logging

from flask import Flask, current_app
from flask_mail import Mail
from flask_wtf.csrf import CSRFProtect

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
from routes.lab_usage import bp as lab_usage_bp
from routes.library import bp as library_bp

mail = Mail()
csrf = CSRFProtect()


def create_app():
    # ---- Logging ----
    log_level = logging.DEBUG if os.getenv("FLASK_DEBUG", "").lower() == "true" else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s [%(name)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    if not os.getenv("FLASK_DEBUG", "").lower() == "true":
        fh = logging.FileHandler("app.log", encoding="utf-8")
        fh.setLevel(logging.WARNING)
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s]: %(message)s"))
        logging.getLogger().addHandler(fh)

    logging.getLogger("waitress.queue").setLevel(logging.ERROR)

    app = Flask(__name__)
    app.config.from_object(Config)

    os.makedirs(app.config["PROFILE_UPLOAD_FOLDER"], exist_ok=True)

    init_appdata()

    csrf.init_app(app)

    sender = app.config.get("MAIL_DEFAULT_SENDER")
    if not sender or (isinstance(sender, tuple) and not sender[1]):
        app.config["MAIL_DEFAULT_SENDER"] = (
            f"Maktabat al-Jamea <{app.config.get('MAIL_USERNAME')}>"
        )

    mail.init_app(app)

    app.logger.info("Mail: server=%s, port=%s, tls=%s",
                    app.config.get("MAIL_SERVER"),
                    app.config.get("MAIL_PORT"),
                    app.config.get("MAIL_USE_TLS"))

    # Jinja helpers
    app.jinja_env.globals.update(
        zip=zip, enumerate=enumerate, len=len, list=list, sorted=sorted,
    )

    with app.app_context():
        from routes.password_reset import setup_mail
        setup_mail()

    # ---- Error Handlers ----
    from flask import render_template

    @app.errorhandler(404)
    def page_not_found(e):
        return render_template("errors/404.html"), 404

    @app.errorhandler(413)
    def file_too_large(e):
        from flask import flash, redirect, request
        flash("❌ File too large. Maximum upload size exceeded.", "danger")
        return redirect(request.referrer or "/"), 413

    @app.errorhandler(500)
    def internal_server_error(e):
        app.logger.error(f"500 Error: {e}", exc_info=True)
        return render_template("errors/500.html"), 500

    # ---- Register Blueprints ----
    app.register_blueprint(auth_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(dashboard_bp, url_prefix="/dashboard")
    app.register_blueprint(hod_dashboard_bp, url_prefix="/hod")
    app.register_blueprint(teacher_dashboard_bp, url_prefix="/teacher")
    app.register_blueprint(reports_bp, url_prefix="/reports")
    app.register_blueprint(student_bp, url_prefix="/students")
    app.register_blueprint(password_reset_bp)
    app.register_blueprint(profile_bp, url_prefix="/profile")
    app.register_blueprint(lab_usage_bp)
    app.register_blueprint(library_bp)

    # ---- Health Check ----
    @app.route("/health")
    def health():
        from db_app import get_conn, close_conn
        db_ok = False
        conn = None
        try:
            conn = get_conn()
            conn.execute("SELECT 1")
            db_ok = True
        except Exception:
            pass
        finally:
            if conn:
                close_conn(conn)

        from tasks.scheduler import is_scheduler_running
        return {
            "status": "healthy" if db_ok else "degraded",
            "database": "connected" if db_ok else "error",
            "scheduler": "running" if is_scheduler_running() else "stopped",
            "version": "2.0.0",
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }

    # ---- Context Processors ----
    @app.context_processor
    def inject_now():
        from services.koha_queries import get_ay_bounds
        start, end = get_ay_bounds()
        current_year = Config.CURRENT_ACADEMIC_YEAR()
        return {
            "now": datetime.datetime.now(datetime.timezone.utc),
            "current_academic_year": current_year,
            "ay_start": start, "ay_end": end,
            "ay_period": f"{start} to {end}" if start and end else "TBD",
        }

    @app.context_processor
    def inject_current_app():
        return dict(current_app=current_app)

    register_scheduler(app, mail)

    # ---- Security Headers ----
    @app.after_request
    def set_security_headers(response):
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "SAMEORIGIN"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        if app.config.get("SESSION_COOKIE_SECURE"):
            response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://code.jquery.com https://cdn.datatables.net https://cdnjs.cloudflare.com; "
            "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://fonts.googleapis.com https://cdn.datatables.net; "
            "font-src 'self' https://fonts.gstatic.com https://cdn.jsdelivr.net; "
            "img-src 'self' data:; "
            "connect-src 'self';"
        )
        return response

    # ---- Teardown: close DB connections ----
    @app.teardown_appcontext
    def close_db(exception):
        # No need to close anything here since we use fresh connections
        pass

    return app


if __name__ == "__main__":
    app = create_app()

    use_waitress = os.getenv("USE_WAITRESS", "false").strip().lower() == "true"
    debug_mode = os.getenv("FLASK_DEBUG", "false").strip().lower() == "true"

    if use_waitress:
        from waitress import serve
        app.logger.info("🚀 Starting Waitress production server on port 5000")
        serve(app, host="0.0.0.0", port=5000, threads=8)
    else:
        app.logger.info("🔧 Starting Flask dev server")
        app.run(debug=debug_mode, host="0.0.0.0", port=5000)
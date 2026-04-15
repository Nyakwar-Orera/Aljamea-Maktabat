# tasks/scheduler.py - FIXED VERSION

from flask_apscheduler import APScheduler
from flask import current_app
from tasks.monthly_reports import send_all_reports
from db_app import get_appdata_conn, close_conn

scheduler = APScheduler()


def _get_email_settings():
    """Fetch scheduler config from SQLite app DB."""
    conn = None
    try:
        conn = get_appdata_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT sender_email, frequency, day_of_week, day_of_month, send_hour, send_minute
            FROM email_settings LIMIT 1
        """)
        row = cur.fetchone()

        if row:
            sender_email, frequency, day_of_week, day_of_month, send_hour, send_minute = row
            return {
                "sender_email": sender_email,
                "frequency": frequency or "monthly",
                "day_of_week": day_of_week or "mon",
                "day_of_month": day_of_month or 1,
                "send_hour": send_hour or 8,
                "send_minute": send_minute or 0,
            }

        # Defaults if not configured
        return {
            "sender_email": None,
            "frequency": "monthly",
            "day_of_week": "mon",
            "day_of_month": 1,
            "send_hour": 8,
            "send_minute": 0,
        }
    except Exception as e:
        import logging
        logging.error(f"Error getting email settings: {e}")
        return {
            "sender_email": None,
            "frequency": "monthly",
            "day_of_week": "mon",
            "day_of_month": 1,
            "send_hour": 8,
            "send_minute": 0,
        }
    finally:
        if conn:
            close_conn(conn)


def _run_reports_job(app, mail):
    """Wrapper to ensure all report emails (darajah + marhala) run inside Flask app context."""
    with app.app_context():
        try:
            app.logger.info("📤 Starting scheduled report job (darajah + marhala)...")
            send_all_reports(app, mail)
            app.logger.info("✅ Scheduled report job completed successfully.")
        except Exception as e:
            app.logger.error(f"❌ Scheduled report job failed: {e}", exc_info=True)


def _register_job(app, mail, settings: dict):
    """Register or re-register the report job based on settings."""
    # Remove old job if exists
    if scheduler.get_job("library-report-job"):
        scheduler.remove_job("library-report-job")

    freq = settings["frequency"]
    hour = settings["send_hour"]
    minute = settings["send_minute"]

    # Choose trigger type
    if freq == "daily":
        scheduler.add_job(
            id="library-report-job",
            func=lambda: _run_reports_job(app, mail),
            trigger="cron",
            hour=hour,
            minute=minute,
            replace_existing=True,
        )
        app.logger.info("📅 Daily report job registered")

    elif freq == "weekly":
        scheduler.add_job(
            id="library-report-job",
            func=lambda: _run_reports_job(app, mail),
            trigger="cron",
            day_of_week=settings["day_of_week"],  # e.g. mon, tue
            hour=hour,
            minute=minute,
            replace_existing=True,
        )
        app.logger.info(f"📅 Weekly report job registered on {settings['day_of_week']}")

    else:  # monthly
        scheduler.add_job(
            id="library-report-job",
            func=lambda: _run_reports_job(app, mail),
            trigger="cron",
            day=settings["day_of_month"],  # admin-chosen day
            hour=hour,
            minute=minute,
            replace_existing=True,
        )
        app.logger.info(f"📅 Monthly report job registered on day {settings['day_of_month']}")


def register_scheduler(app, mail):
    """Initialize and start the APScheduler with current app settings."""
    if not scheduler.running:
        scheduler.init_app(app)
        settings = _get_email_settings()
        _register_job(app, mail, settings)
        # 🤖 AI Nudge Job (personalized recommendations)
        from tasks.ai_nudge import send_ai_nudges
        scheduler.add_job(
            id="ai-nudge-job",
            func=lambda: send_ai_nudges(app, mail),
            trigger="cron",
            day_of_week="mon",
            hour=9,
            minute=0,
            replace_existing=True,
        )
        app.logger.info("📅 AI Nudge job registered for Mondays at 09:00 AM")
        
        scheduler.start()
        app.logger.info("✅ APScheduler started successfully.")
    return scheduler


def reload_scheduler(app, mail):
    """Reload scheduler when admin updates email_settings from UI."""
    settings = _get_email_settings()
    _register_job(app, mail, settings)
    app.logger.info(f"🔄 Scheduler reloaded with settings: {settings}")


def is_scheduler_running():
    """Check if scheduler is currently running."""
    return scheduler.running


def stop_scheduler():
    """Stop the scheduler if it's running."""
    if scheduler.running:
        scheduler.shutdown()
        return True
    return False


def get_scheduled_jobs():
    """Get list of scheduled jobs."""
    return scheduler.get_jobs()
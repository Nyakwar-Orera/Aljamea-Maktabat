# tasks/ai_nudge.py — FIXED: Dynamic marhala per student
import logging
from flask_mail import Message
from flask import render_template
from services.recommendation_service import RecommendationService
from db_koha import koha_conn

logger = logging.getLogger(__name__)


def _get_student_marhala(student_cardnumber: str) -> str:
    """Look up the student\'s actual marhala/category from Koha."""
    try:
        with koha_conn() as conn:
            cur = conn.cursor(dictionary=True)
            cur.execute("""
                SELECT c.description
                FROM borrowers b
                JOIN categories c ON b.categorycode = c.categorycode
                WHERE b.cardnumber = %s
                LIMIT 1
            """, (student_cardnumber,))
            row = cur.fetchone()
            if row and row.get("description"):
                return row["description"]
    except Exception as e:
        logger.warning(f"Could not fetch marhala for {student_cardnumber}: {e}")
    return "Darajah 5-7"  # Fallback


def send_ai_nudges(app, mail):
    """Send personalized book recommendations to lapsed borrowers."""
    with app.app_context():
        try:
            logger.info("📤 Starting AI Nudge Email Job...")
            lapsed_students = RecommendationService.get_lapsed_borrowers(months_threshold=3)
            sent_count = 0
            error_count = 0

            for student in lapsed_students:
                email = student.get("email")
                username = student.get("cardnumber")
                first_name = student.get("firstname", "Student")

                if not email:
                    continue

                try:
                    # Get student\'s ACTUAL marhala
                    marhala = _get_student_marhala(username)
                    recommendations = RecommendationService.get_marhala_recommendations(marhala, limit=3)

                    if not recommendations:
                        continue

                    msg = Message(
                        subject="📚 We\'ve missed you at Maktabat al-Jamea!",
                        recipients=[email],
                        sender=app.config.get("MAIL_DEFAULT_SENDER"),
                    )
                    msg.html = render_template(
                        "emails/ai_nudge.html",
                        first_name=first_name,
                        books=recommendations,
                    )
                    mail.send(msg)
                    sent_count += 1
                    logger.info(f"✅ Nudge sent to {email}")

                except Exception as e:
                    error_count += 1
                    logger.error(f"❌ Failed to send nudge to {email}: {e}")

            logger.info(f"📊 AI Nudge complete: {sent_count} sent, {error_count} errors")

        except Exception as e:
            logger.error(f"❌ AI Nudge job failed: {e}", exc_info=True)

import logging
from flask_mail import Message
from flask import render_template
from services.recommendation_service import RecommendationService
from config import Config

logger = logging.getLogger(__name__)

def send_ai_nudges(app, mail):
    """
    Periodic task to send personalized book recommendations and nudges.
    This identifies students who haven't borrowed in 3 months and sends 
    a 'Welcome Back' email with trending titles in their Marhala.
    """
    with app.app_context():
        try:
            logger.info("📤 Starting AI Nudge Email Job...")
            # 1. Identify lapsed borrowers (e.g. students who haven't borrowed in 3 months)
            lapsed_students = RecommendationService.get_lapsed_borrowers(months_threshold=3)
            
            for student in lapsed_students:
                email = student['email']
                username = student['cardnumber']
                first_name = student['firstname']
                
                # 2. Get recommendations for this student category (Marhala)
                # Need to find the student's category from Koha or local mapping
                # For now, let's assume we can find their Marhala from Koha directly
                # To be efficient, we can fetch all categories once.
                
                # Fetch trending books for this student
                recommendations = RecommendationService.get_marhala_recommendations("Darajah 5-7") # Example marhala
                
                if recommendations:
                    msg = Message(
                        subject="📚 We've missed you at Maktabat al-Jamea!",
                        recipients=[email],
                        sender=app.config.get("MAIL_DEFAULT_SENDER")
                    )
                    
                    # Create personalized message body
                    msg.html = render_template(
                        "emails/ai_nudge.html",
                        first_name=first_name,
                        books=recommendations[:3] # Show top 3
                    )
                    
                    mail.send(msg)
                    logger.info(f"✅ Nudge email sent to {email} ({username})")
            
            logger.info(f"✨ AI Nudge job completed. {len(lapsed_students)} nudges processed.")
            
        except Exception as e:
            logger.error(f"❌ AI Nudge job failed: {e}", exc_info=True)

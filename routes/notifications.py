# routes/notifications.py
from flask import Blueprint, jsonify, session, request
from db_app import get_conn

bp = Blueprint("notifications_bp", __name__)

# Get notifications
@bp.route("/api/notifications", methods=["GET"])
def get_notifications():
    """Fetch notifications for the logged-in user."""
    if not session.get("logged_in"):
        return jsonify([])  # Return empty if not logged in
    
    username = session.get("username")
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, message, status FROM notifications WHERE user = ? ORDER BY created_at DESC
    """, (username,))
    notifications = cur.fetchall()
    conn.close()

    return jsonify([
        {
            "id": n[0],
            "message": n[1],
            "status": n[2],
        }
        for n in notifications
    ])


# Mark notification as read
@bp.route("/api/mark_notification_read", methods=["POST"])
def mark_notification_read():
    """Mark notification as read."""
    if not session.get("logged_in"):
        return jsonify({"success": False})
    
    notification_id = request.form.get("id")
    
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE notifications SET status = 'read' WHERE id = ?", (notification_id,))
    conn.commit()
    conn.close()
    
    return jsonify({"success": True})

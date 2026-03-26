import random
import logging
from typing import List, Dict, Any
from db_koha import get_koha_conn, koha_conn
from services.koha_queries import get_ay_bounds

logger = logging.getLogger(__name__)

class RecommendationService:
    @staticmethod
    def get_marhala_recommendations(marhala_name: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Identify trending titles within a specific marhala/category."""
        with koha_conn() as conn:
            cur = conn.cursor(dictionary=True)
            # Find titles borrowed by people in this marhala
            query = """
                SELECT bib.biblionumber, bib.title, bib.author, COUNT(*) as borrow_count
                FROM statistics s
                JOIN borrowers b ON s.borrowernumber = b.borrowernumber
                JOIN categories c ON b.categorycode = c.categorycode
                JOIN items it ON s.itemnumber = it.itemnumber
                JOIN biblio bib ON it.biblionumber = bib.biblionumber
                WHERE c.description = %s AND s.type = 'issue'
                GROUP BY bib.biblionumber, bib.title, bib.author
                ORDER BY borrow_count DESC
                LIMIT %s
            """
            cur.execute(query, (marhala_name, limit))
            return cur.fetchall()

    @staticmethod
    def get_personalized_recommendations(student_username: str, limit: int = 3) -> List[Dict[str, Any]]:
        """Recommend books based on a student's past genres/authors."""
        # Simple implementation: find most borrowed author by student, then recommend other books by that author
        # In a real system, we'd use Collaborative Filtering.
        with koha_conn() as conn:
            cur = conn.cursor(dictionary=True)
            
            # 1. Get student's top author
            cur.execute("""
                SELECT bib.author, COUNT(*) as count
                FROM statistics s
                JOIN borrowers b ON s.borrowernumber = b.borrowernumber
                JOIN items it ON s.itemnumber = it.itemnumber
                JOIN biblio bib ON it.biblionumber = bib.biblionumber
                WHERE b.cardnumber = %s AND s.type = 'issue' AND bib.author IS NOT NULL AND bib.author != ''
                GROUP BY bib.author
                ORDER BY count DESC
                LIMIT 1
            """, (student_username,))
            top_author_row = cur.fetchone()
            
            if not top_author_row:
                return []

            top_author = top_author_row['author']
            
            # 2. Find other books by this author that student hasn't read
            cur.execute("""
                SELECT DISTINCT bib.biblionumber, bib.title, bib.author
                FROM biblio bib
                JOIN items it ON bib.biblionumber = it.biblionumber
                WHERE bib.author = %s
                AND bib.biblionumber NOT IN (
                    SELECT it2.biblionumber
                    FROM statistics s2
                    JOIN borrowers b2 ON s2.borrowernumber = b2.borrowernumber
                    JOIN items it2 ON s2.itemnumber = it2.itemnumber
                    WHERE b2.cardnumber = %s AND s2.type = 'issue'
                )
                LIMIT %s
            """, (top_author, student_username, limit))
            
            return cur.fetchall()

    @staticmethod
    def get_lapsed_borrowers(months_threshold: int = 3, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """Identify students who haven't borrowed anything in the last few months with pagination."""
        with koha_conn() as conn:
            cur = conn.cursor(dictionary=True)
            query = """
                SELECT b.cardnumber, b.firstname, b.surname, b.email, 
                       COALESCE(MAX(s.datetime), 'Never') as last_borrowed
                FROM borrowers b
                JOIN categories c ON b.categorycode = c.categorycode
                LEFT JOIN statistics s ON b.borrowernumber = s.borrowernumber AND s.type = 'issue'
                WHERE c.description IN ('Darajah 1-2', 'Darajah 3-4', 'Darajah 5-7', 'Darajah 8-11')
                GROUP BY b.borrowernumber
                HAVING (MAX(s.datetime) < DATE_SUB(CURDATE(), INTERVAL %s MONTH) OR MAX(s.datetime) IS NULL)
                AND b.email IS NOT NULL AND b.email != ''
                LIMIT %s OFFSET %s
            """
            cur.execute(query, (months_threshold, limit, offset))
            return cur.fetchall()

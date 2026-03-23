-- sql/koha.sql
-- UPDATED: 2026-02-13
-- PURPOSE: Koha reporting queries for Marhala/Darajah analytics
-- 
-- PLACEHOLDER NOTICE: All queries use '?' for DBI prepared statements, NOT '%s'

-- ==========================================
-- PATRON & ISSUE ANALYTICS
-- ==========================================

-- name: patron_title_agg_between_dates
-- purpose: Per-patron title aggregation with first issue date within a date range
-- params: from_date (DATE), to_date (DATE), exclude_category (e.g., 'T-KG')
-- UPDATED: Added active patron filters, fixed GROUP BY for MySQL 5.7+
SELECT
  b.borrowernumber,
  b.cardnumber,
  CONCAT_WS(' ', b.surname, b.firstname) AS patron_name,
  COALESCE(std.attribute, 'Unknown') AS class_std,
  trno.attribute AS trno,
  COALESCE(COUNT(DISTINCT d.biblionumber), 0) AS issued_count,
  GROUP_CONCAT(
      DISTINCT CONCAT(bib.title, ' (', DATE_FORMAT(d.first_issued, '%d-%b-%Y'), ')')
      ORDER BY bib.title SEPARATOR ' • '
  ) AS titles_list
FROM borrowers b
LEFT JOIN borrower_attributes std
    ON std.borrowernumber = b.borrowernumber 
    AND std.code = 'STD'
LEFT JOIN borrower_attributes trno
    ON trno.borrowernumber = b.borrowernumber 
    AND trno.code = 'TRNO'
LEFT JOIN (
    SELECT s.borrowernumber, it.biblionumber, MIN(DATE(s.datetime)) AS first_issued
    FROM statistics s
    JOIN items it ON it.itemnumber = s.itemnumber
    WHERE s.type = 'issue'
        AND DATE(s.datetime) BETWEEN ? AND ?
    GROUP BY s.borrowernumber, it.biblionumber
) d ON d.borrowernumber = b.borrowernumber
LEFT JOIN biblio bib ON bib.biblionumber = d.biblionumber
WHERE b.categorycode <> ?
    AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
    AND (b.debarred IS NULL OR b.debarred = 0)
    AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
GROUP BY 
    b.borrowernumber, 
    b.cardnumber, 
    patron_name, 
    class_std, 
    trno.attribute
ORDER BY class_std, patron_name;

-- ==========================================
-- TITLE POPULARITY
-- ==========================================

-- name: top_borrowed_titles
-- purpose: Top-N borrowed titles with language filter
-- params: lang_filter (VARCHAR), limit (INT)
-- UPDATED: Fixed GROUP BY, added biblionumber to avoid title collisions
SELECT
    bi.biblionumber,
    bi.title,
    COUNT(DISTINCT s.datetime) AS times_borrowed,
    MAX(DATE(s.datetime)) AS last_issued,
    GROUP_CONCAT(DISTINCT bii.language SEPARATOR ', ') AS languages
FROM statistics s
JOIN items it ON it.itemnumber = s.itemnumber
JOIN biblio bi ON bi.biblionumber = it.biblionumber
LEFT JOIN biblioitems bii ON bii.biblionumber = bi.biblionumber
WHERE s.type = 'issue'
    AND (? IS NULL OR bii.language = ? OR bii.language LIKE CONCAT(?, '%'))
GROUP BY bi.biblionumber, bi.title
ORDER BY times_borrowed DESC
LIMIT ?;

-- name: top_titles_by_language_marc
-- purpose: Top titles by MARC 041$a language code (exact match)
-- params: language_code (VARCHAR), limit (INT)
-- UPDATED: New consolidated query replacing arabic_top25/english_top25
SELECT
    bib.biblionumber AS Biblio_ID,
    bib.title AS Title,
    ExtractValue(
        bmd.metadata,
        '//datafield[@tag="041"]/subfield[@code="a"]'
    ) AS Language,
    GROUP_CONCAT(DISTINCT it.ccode ORDER BY it.ccode SEPARATOR ', ') AS Collections,
    COUNT(DISTINCT i.issue_id) AS Times_Issued,
    MAX(i.issuedate) AS Last_Issued
FROM issues i
INNER JOIN items it ON i.itemnumber = it.itemnumber
INNER JOIN biblio bib ON it.biblionumber = bib.biblionumber
INNER JOIN biblio_metadata bmd ON bib.biblionumber = bmd.biblionumber
WHERE ExtractValue(
        bmd.metadata,
        '//datafield[@tag="041"]/subfield[@code="a"]'
    ) = ?  -- Exact match, not LIKE
GROUP BY bib.biblionumber, bib.title, Language
ORDER BY Times_Issued DESC
LIMIT ?;

-- name: arabic_top25
-- purpose: Top 25 Arabic books (wrapper for top_titles_by_language_marc)
-- params: none
-- NOTE: This is a convenience wrapper - consider deprecating
SELECT * FROM top_titles_by_language_marc('Arabic', 25);

-- name: english_top25
-- purpose: Top 25 English books (wrapper for top_titles_by_language_marc)
-- params: none
-- NOTE: This is a convenience wrapper - consider deprecating
SELECT * FROM top_titles_by_language_marc('English', 25);

-- ==========================================
-- SIP2 ACTIVITY
-- ==========================================

-- name: sip_activity_counts
-- purpose: SIP2 events by type over a rolling window (days)
-- params: days_window (INT)
-- UPDATED: Added ordering
SELECT 
    s.type, 
    COUNT(*) AS events,
    MIN(DATE(s.datetime)) AS earliest,
    MAX(DATE(s.datetime)) AS latest
FROM statistics s
WHERE s.datetime >= (CURRENT_DATE - INTERVAL ? DAY)
    AND s.interface = 'SIP2'
GROUP BY s.type
ORDER BY events DESC;

-- ==========================================
-- CLASS/DARAJAH ANALYTICS
-- ==========================================

-- name: class_issue_counts_by_std
-- purpose: Count issues grouped by STD borrower attribute (class)
-- params: none (optional: from_date, to_date)
-- UPDATED: Added date range parameters (optional), fixed COUNT logic
SELECT 
    COALESCE(std.attribute, 'Unknown') AS class_name,
    COUNT(DISTINCT iss.issue_id) AS issues,
    COUNT(DISTINCT iss.borrowernumber) AS active_patrons
FROM borrowers b
LEFT JOIN borrower_attributes std
    ON std.borrowernumber = b.borrowernumber 
    AND std.code = 'STD'
LEFT JOIN issues iss 
    ON iss.borrowernumber = b.borrowernumber
    AND (iss.issuedate BETWEEN ? AND ? OR ? IS NULL)
WHERE (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
    AND (b.debarred IS NULL OR b.debarred = 0)
    AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
GROUP BY class_name
ORDER BY issues DESC;

-- name: patron_list_by_class_std
-- purpose: List patrons in a given class (STD attribute)
-- params: class_std (VARCHAR)
-- UPDATED: Fixed WHERE clause logic, added patron status
SELECT
    b.borrowernumber,
    b.cardnumber,
    CONCAT(b.surname, ' ', b.firstname) AS FullName,
    b.email AS EduEmail,
    b.categorycode,
    c.description AS category,
    std.attribute AS class,
    COALESCE(x.total_issues, 0) AS TotalIssues,
    COALESCE(x.fees_paid, 0.00) AS TotalFeesPaid,
    CASE 
        WHEN b.dateexpiry < CURDATE() THEN 'Expired'
        WHEN b.debarred = 1 THEN 'Debarred'
        WHEN b.gonenoaddress = 1 THEN 'Address Lost'
        ELSE 'Active'
    END AS patron_status
FROM borrowers b
LEFT JOIN categories c ON c.categorycode = b.categorycode
LEFT JOIN borrower_attributes std
    ON std.borrowernumber = b.borrowernumber 
    AND std.code = 'STD'
LEFT JOIN (
    SELECT 
        iss.borrowernumber,
        COUNT(*) AS total_issues,
        COALESCE(SUM(CASE 
            WHEN al.credit_type_code = 'PAYMENT' 
                 AND (al.status IS NULL OR al.status != 'VOID')
            THEN al.amount 
            ELSE 0 
        END), 0) AS fees_paid
    FROM issues iss
    LEFT JOIN accountlines al ON iss.issue_id = al.issue_id  -- Fixed JOIN condition
    GROUP BY iss.borrowernumber
) x ON x.borrowernumber = b.borrowernumber
WHERE std.attribute = ?  -- Removed OR branchcode - use class only
    AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
    AND (b.debarred IS NULL OR b.debarred = 0)
    AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
ORDER BY FullName ASC;

-- name: patron_list_by_department
-- purpose: List patrons in a department (by Koha category)
-- params: dept_code (VARCHAR), dept_desc (VARCHAR)
-- UPDATED: Split parameters for clarity
SELECT
    b.borrowernumber,
    b.cardnumber,
    CONCAT(b.surname, ' ', b.firstname) AS FullName,
    b.email AS EduEmail,
    b.categorycode,
    c.description AS category,
    COALESCE(std.attribute, b.branchcode) AS class,
    COALESCE(x.total_issues, 0) AS TotalIssues,
    COALESCE(x.fees_paid, 0.00) AS TotalFeesPaid,
    b.dateexpiry
FROM borrowers b
LEFT JOIN categories c ON c.categorycode = b.categorycode
LEFT JOIN borrower_attributes std
    ON std.borrowernumber = b.borrowernumber 
    AND std.code = 'STD'
LEFT JOIN (
    SELECT 
        iss.borrowernumber,
        COUNT(*) AS total_issues,
        COALESCE(SUM(CASE 
            WHEN al.credit_type_code = 'PAYMENT' 
            THEN al.amount 
            ELSE 0 
        END), 0) AS fees_paid
    FROM issues iss
    LEFT JOIN accountlines al ON iss.issue_id = al.issue_id
    GROUP BY iss.borrowernumber
) x ON x.borrowernumber = b.borrowernumber
WHERE (c.description = ? OR b.categorycode = ?)
    AND (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
    AND (b.debarred IS NULL OR b.debarred = 0)
    AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
ORDER BY FullName ASC;

-- ==========================================
-- DARAJAH BUCKETING (CONSOLIDATED)
-- ==========================================

-- name: darajah_buckets_from_std
-- purpose: Bucket patrons into Darajah groups based on numeric STD attribute
-- params: none
-- UPDATED: Consistent naming (Darajah), added active patron filter
SELECT
    CASE
        WHEN std.attribute REGEXP '^[0-9]+$' THEN
            CASE
                WHEN CAST(std.attribute AS UNSIGNED) BETWEEN 1 AND 2 THEN 'Darajah 1–2'
                WHEN CAST(std.attribute AS UNSIGNED) BETWEEN 3 AND 4 THEN 'Darajah 3–4'
                WHEN CAST(std.attribute AS UNSIGNED) BETWEEN 5 AND 7 THEN 'Darajah 5–7'
                WHEN CAST(std.attribute AS UNSIGNED) BETWEEN 8 AND 11 THEN 'Darajah 8–11'
                ELSE 'Other'
            END
        ELSE 'Unassigned'
    END AS darajah_group,
    COUNT(*) AS patrons,
    SUM(CASE 
        WHEN (b.dateexpiry IS NULL OR b.dateexpiry >= CURDATE())
            AND (b.debarred IS NULL OR b.debarred = 0)
            AND (b.gonenoaddress IS NULL OR b.gonenoaddress = 0)
        THEN 1 ELSE 0 
    END) AS active_patrons
FROM borrowers b
LEFT JOIN borrower_attributes std
    ON std.borrowernumber = b.borrowernumber 
    AND std.code = 'STD'
GROUP BY darajah_group
ORDER BY 
    FIELD(darajah_group,
        'Darajah 1–2', 
        'Darajah 3–4', 
        'Darajah 5–7', 
        'Darajah 8–11', 
        'Other', 
        'Unassigned'
    );

-- name: darajah_max_books
-- purpose: Maximum concurrent issues allowed per Darajah group
-- params: none
-- UPDATED: Consistent with bucket groups
SELECT 'Darajah 1–2' AS darajah_group, 3 AS max_books
UNION ALL
SELECT 'Darajah 3–4', 4 AS max_books
UNION ALL
SELECT 'Darajah 5–7', 5 AS max_books
UNION ALL
SELECT 'Darajah 8–11', 6 AS max_books
UNION ALL
SELECT 'Other', 2 AS max_books
UNION ALL
SELECT 'Unassigned', 1 AS max_books;

-- ==========================================
-- ADDITIONAL USEFUL QUERIES
-- ==========================================

-- name: academic_year_issue_summary
-- purpose: Summary of issues for current academic year by month
-- params: start_date (DATE), end_date (DATE)
SELECT
    DATE_FORMAT(s.datetime, '%Y-%m') AS month,
    COUNT(*) AS total_issues,
    COUNT(DISTINCT s.borrowernumber) AS unique_borrowers,
    COUNT(DISTINCT it.biblionumber) AS unique_titles,
    AVG(DATEDIFF(s.datetime, s.datetime)) AS avg_loan_duration  -- Placeholder
FROM statistics s
JOIN items it ON s.itemnumber = it.itemnumber
WHERE s.type = 'issue'
    AND DATE(s.datetime) BETWEEN ? AND ?
GROUP BY DATE_FORMAT(s.datetime, '%Y-%m')
ORDER BY month;

-- name: marhala_darajah_cross_tab
-- purpose: Cross-tabulation of Marhala (category) vs Darajah (STD)
-- params: from_date (DATE), to_date (DATE)
SELECT
    c.description AS marhala,
    COALESCE(std.attribute, 'Unknown') AS darajah,
    COUNT(DISTINCT s.borrowernumber) AS active_patrons,
    COUNT(*) AS total_issues
FROM statistics s
JOIN borrowers b ON s.borrowernumber = b.borrowernumber
JOIN categories c ON b.categorycode = c.categorycode
LEFT JOIN borrower_attributes std
    ON std.borrowernumber = b.borrowernumber 
    AND std.code = 'STD'
WHERE s.type = 'issue'
    AND DATE(s.datetime) BETWEEN ? AND ?
GROUP BY c.description, darajah
ORDER BY c.description, 
    CAST(REGEXP_SUBSTR(darajah, '^[0-9]+') AS UNSIGNED);

-- ==========================================
-- INDEX RECOMMENDATIONS
-- ==========================================
/*
-- Run these in Koha database to improve query performance:

CREATE INDEX idx_statistics_type_datetime ON statistics (type, DATE(datetime));
CREATE INDEX idx_borrower_attributes_code_value ON borrower_attributes (code, attribute(50));
CREATE INDEX idx_issues_borrowernumber_issuedate ON issues (borrowernumber, issuedate);
CREATE INDEX idx_old_issues_borrowernumber_issuedate ON old_issues (borrowernumber, issuedate);
CREATE INDEX idx_borrowers_category_status ON borrowers (categorycode, dateexpiry, debarred, gonenoaddress);
*/
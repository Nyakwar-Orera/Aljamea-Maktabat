-- name: patron_title_agg_between_dates
-- purpose: Per-patron title aggregation with first issue date within a date range
-- params: from_date (DATE), to_date (DATE), exclude_category (e.g., 'T-KG')
SELECT
  b.borrowernumber,
  b.cardnumber,
  CONCAT_WS(' ', b.surname, b.firstname)        AS patron_name,
  std.attribute                                  AS class_std,
  trno.attribute                                 AS trno,
  COALESCE(COUNT(d.biblionumber),0)              AS issued_count,
  GROUP_CONCAT(
      CONCAT(bib.title,' (',DATE_FORMAT(d.first_issued,'%d-%b-%Y'),')')
      ORDER BY bib.title SEPARATOR ' • '
  )                                              AS titles_list
FROM borrowers b
LEFT JOIN borrower_attributes std
       ON std.borrowernumber = b.borrowernumber AND std.code = 'STD'
LEFT JOIN borrower_attributes trno
       ON trno.borrowernumber = b.borrowernumber AND trno.code = 'TRNO'
LEFT JOIN (
  /* one row per (borrower, title) issued in period */
  SELECT s.borrowernumber, it.biblionumber, MIN(DATE(s.datetime)) AS first_issued
  FROM statistics s
  JOIN items it ON it.itemnumber = s.itemnumber
  WHERE s.type = 'issue'
    AND DATE(s.datetime) BETWEEN %s AND %s
  GROUP BY s.borrowernumber, it.biblionumber
) d ON d.borrowernumber = b.borrowernumber
LEFT JOIN biblio bib ON bib.biblionumber = d.biblionumber
WHERE b.categorycode <> %s
GROUP BY b.borrowernumber
ORDER BY class_std, patron_name;

-- name: top_borrowed_titles
-- purpose: Top-N borrowed titles overall or filtered by language code (from biblioitems.language)
-- params: lang_filter (nullable VARCHAR), limit (INT)
SELECT
  bi.title,
  COUNT(*) AS times_borrowed,
  MAX(DATE(s.datetime)) AS last_issued
FROM statistics s
JOIN items it        ON it.itemnumber   = s.itemnumber
JOIN biblio bi       ON bi.biblionumber = it.biblionumber
LEFT JOIN biblioitems bii ON bii.biblionumber = bi.biblionumber
WHERE s.type = 'issue'
  AND (%s IS NULL OR bii.language = %s OR bii.language LIKE CONCAT(%s, '%%'))
GROUP BY bi.title
ORDER BY times_borrowed DESC
LIMIT %s;

-- name: sip_activity_counts
-- purpose: SIP2 events by type over a rolling window (days)
-- params: days_window (INT)
SELECT s.type, COUNT(*) AS events
FROM statistics s
WHERE s.datetime >= (CURRENT_DATE - INTERVAL %s DAY)
  AND s.interface = 'SIP2'
GROUP BY s.type;

-- name: class_issue_counts_by_std
-- purpose: Count issues grouped by STD borrower attribute (class), with Unknown fallback
-- params: none
SELECT COALESCE(std.attribute, 'Unknown') AS class_name,
       COUNT(iss.issue_id) AS issues
FROM borrowers b
LEFT JOIN borrower_attributes std
  ON std.borrowernumber = b.borrowernumber AND std.code = 'STD'
LEFT JOIN issues iss ON iss.borrowernumber = b.borrowernumber
GROUP BY class_name
ORDER BY issues DESC;

-- name: patron_list_by_class_std
-- purpose: List patrons in a given class (STD), with totals
-- params: class_std (VARCHAR)
SELECT
  b.borrowernumber,
  b.cardnumber,
  CONCAT(b.surname, ' ', b.firstname) AS FullName,
  b.email AS EduEmail,
  b.categorycode,
  c.description AS category,
  COALESCE(std.attribute, b.branchcode) AS class,
  COALESCE(x.total_issues, 0) AS TotalIssues,
  COALESCE(x.fines_paid, 0) AS TotalFinesPaid
FROM borrowers b
LEFT JOIN categories c ON c.categorycode = b.categorycode
LEFT JOIN borrower_attributes std
  ON std.borrowernumber = b.borrowernumber AND std.code = 'STD'
LEFT JOIN (
  SELECT iss.borrowernumber,
         COUNT(*) AS total_issues,
         COALESCE(SUM(CASE WHEN al.credit_type_code='PAYMENT' THEN al.amount END),0) AS fines_paid
  FROM issues iss
  LEFT JOIN accountlines al ON iss.borrowernumber = al.borrowernumber
  GROUP BY iss.borrowernumber
) x ON x.borrowernumber = b.borrowernumber
WHERE std.attribute = %s OR b.branchcode = %s
ORDER BY FullName ASC;

-- name: patron_list_by_department
-- purpose: List patrons in a department (by Koha category or description)
-- params: dept (VARCHAR)
SELECT
  b.borrowernumber,
  b.cardnumber,
  CONCAT(b.surname, ' ', b.firstname) AS FullName,
  b.email AS EduEmail,
  b.categorycode,
  c.description AS category,
  COALESCE(std.attribute, b.branchcode) AS class,
  COALESCE(x.total_issues, 0) AS TotalIssues,
  COALESCE(x.fines_paid, 0) AS TotalFinesPaid
FROM borrowers b
LEFT JOIN categories c ON c.categorycode = b.categorycode
LEFT JOIN borrower_attributes std
  ON std.borrowernumber = b.borrowernumber AND std.code = 'STD'
LEFT JOIN (
  SELECT iss.borrowernumber,
         COUNT(*) AS total_issues,
         COALESCE(SUM(CASE WHEN al.credit_type_code='PAYMENT' THEN al.amount END),0) AS fines_paid
  FROM issues iss
  LEFT JOIN accountlines al ON iss.borrowernumber = al.borrowernumber
  GROUP BY iss.borrowernumber
) x ON x.borrowernumber = b.borrowernumber
WHERE (c.description = %s OR b.categorycode = %s)
ORDER BY FullName ASC;

-- name: daraja_buckets_from_std
-- purpose: Bucket students into Daraja groups based on numeric STD attribute
-- params: none
SELECT
  CASE
    WHEN CAST(std.attribute AS UNSIGNED) BETWEEN 1 AND 2  THEN 'Daraja 1–2'
    WHEN CAST(std.attribute AS UNSIGNED) BETWEEN 3 AND 4  THEN 'Daraja 3–4'
    WHEN CAST(std.attribute AS UNSIGNED) BETWEEN 5 AND 7  THEN 'Daraja 5–7'
    WHEN CAST(std.attribute AS UNSIGNED) BETWEEN 8 AND 11 THEN 'Daraja 8–11'
    ELSE 'Unassigned'
  END AS daraja_group,
  COUNT(*) AS patrons
FROM borrowers b
LEFT JOIN borrower_attributes std
  ON std.borrowernumber = b.borrowernumber AND std.code = 'STD'
GROUP BY daraja_group
ORDER BY MIN(CASE daraja_group
  WHEN 'Daraja 1–2' THEN 1
  WHEN 'Daraja 3–4' THEN 2
  WHEN 'Daraja 5–7' THEN 3
  WHEN 'Daraja 8–11' THEN 4
  ELSE 9 END);

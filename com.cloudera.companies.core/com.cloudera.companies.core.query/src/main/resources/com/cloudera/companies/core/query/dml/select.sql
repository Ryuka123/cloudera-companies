--
-- Count companies accross a specific snapshot with a like matching name
--

SELECT
  count(1) AS company_count
FROM company
WHERE
  upper(company_name) LIKE '%01 PROPERTY INVESTMENT%' AND
  snapshot_year='2012' AND snapshot_month='05';

--
-- Select companies accross a specific snapshot with a like matching name
--

SELECT
  *
FROM company
WHERE
  upper(company_name) LIKE '%01 PROPERTY INVESTMENT%' AND
  snapshot_year='2012' AND snapshot_month='05'
ORDER BY company_name
LIMIT 100;


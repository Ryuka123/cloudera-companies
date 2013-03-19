--
-- Count duplicate companies accross a specific snapshot
--

SELECT
  count(1) AS company_duplicate_count
FROM company_duplicate
WHERE
  snapshot_year='2012' AND snapshot_month='05';

--
-- Count duplicate companies accross a specific snapshot
--

SELECT
  count(1) AS company_duplicate_count
FROM company
INNER JOIN company_duplicate ON (
  company.company_name = company_duplicate.company_name
)
WHERE
  company.snapshot_year='2012' AND company.snapshot_month='05' AND
  company_duplicate.snapshot_year='2012' AND company_duplicate.snapshot_month='05';

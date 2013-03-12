--
-- Count duplicate companies accross all snapshots
--

SELECT
  count(1) AS company_duplicate_count
FROM company_duplicate;

--
-- Count duplicate companies accross a specific snapshot
--

SELECT
  count(1) AS company_duplicate_count
FROM company_duplicate
WHERE
  snapshot_year='2012' AND
  snapshot_month='05';


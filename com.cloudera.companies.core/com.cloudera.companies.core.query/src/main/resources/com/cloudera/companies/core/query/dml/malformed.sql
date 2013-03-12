--
-- Count malformed companies accross all snapshots
--

SELECT
  count(1) AS company_malformed_count
FROM company_malformed;

--
-- Count malformed companies accross a specific snapshot
--

SELECT
  count(1) AS company_malformed_count
FROM company_malformed
WHERE
  snapshot_year='2012' AND
  snapshot_month='05';


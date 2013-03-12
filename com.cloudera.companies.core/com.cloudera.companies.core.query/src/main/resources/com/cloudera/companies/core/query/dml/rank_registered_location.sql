--
-- Count companies registered 
--

SELECT
  unix_timestamp(incorporation_date, 'dd/mm/yyyy') as incorporation_year
FROM company
WHERE snapshot_year=2012 AND snapshot_month=05
-- GROUP BY to_date(incorporation_date)
-- ORDER BY incorporation_year DESC
LIMIT 5;

SELECT
  '2009-03-20',
  unix_timestamp('2009-03-20', 'yyyy-MM-dd')
FROM company
LIMIT 1;

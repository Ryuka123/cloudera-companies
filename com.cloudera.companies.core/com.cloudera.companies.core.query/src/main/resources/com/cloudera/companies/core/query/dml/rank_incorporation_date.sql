--
-- Count companies registered 
--

SELECT
  regexp_replace(upper(reg_address_post_code), '([0-z]+[ ][0-9])[A-Z]*', '$1') as registered_location,
  count(reg_address_post_code) AS count
FROM company
WHERE snapshot_year=2012 AND snapshot_month=05
GROUP BY reg_address_post_code
ORDER BY count DESC
LIMIT 5;



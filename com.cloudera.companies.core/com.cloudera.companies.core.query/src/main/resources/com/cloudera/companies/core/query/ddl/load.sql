--
-- Schema Load
--

LOAD DATA INPATH '/Users/graham/_/dev/personal/cloudera-companies/com.cloudera.companies.core/com.cloudera.companies.core.query/target/test-hdfs/tmp/companies/processed/cleansed/2012/05/MAY-2012-r-00000'
OVERWRITE INTO TABLE Company
PARTITION (
	Year='2012',
	Month='05'
);

LOAD DATA INPATH '/Users/graham/_/dev/personal/cloudera-companies/com.cloudera.companies.core/com.cloudera.companies.core.query/target/test-hdfs/tmp/companies/processed/cleansed/2012/06/JUN-2012-r-00000'
OVERWRITE INTO TABLE Company
PARTITION (
	Year='2012',
	Month='06'
);

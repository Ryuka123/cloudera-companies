--
-- Schema Create
--

-- Delimited table

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:company.data.table} (
  company_name STRING,
  company_number STRING,
  reg_address_care_of STRING,
  reg_address_po_box STRING,
  reg_address_address_line_1 STRING,
  reg_address_address_line_2 STRING,
  reg_address_post_town STRING,
  reg_address_county STRING,
  reg_address_country STRING,
  reg_address_post_code STRING,
  company_category STRING,
  company_status STRING,
  country_of_origin STRING,
  dissolution_date STRING,
  incorporation_date STRING,
  accounts_account_ref_day STRING,
  accounts_account_ref_month STRING,
  accounts_next_due_date STRING,
  accounts_last_made_up_date STRING,
  accounts_account_category STRING,
  returns_next_due_date STRING,
  returns_last_made_up_date STRING,
  mortgages_num_mort_charges STRING,
  mortgages_num_mort_outstanding STRING,
  mortgages_num_mort_part_satisfied STRING,
  mortgages_num_mort_satisfied STRING,
  sic_code_sic_text_1 STRING,
  sic_code_sic_text_2 STRING,
  sic_code_sic_text_3 STRING,
  sic_code_sic_text_4 STRING,
  limited_partnerships_numGenPartners STRING,
  limited_partnerships_numLimPartners STRING,
  uri STRING,
  previous_name_1_condate STRING,
  previous_name_1_company_name STRING,
  previous_name_2_condate STRING,
  previous_name_2_company_name STRING,
  previous_name_3_condate STRING,
  previous_name_3_company_name STRING,
  previous_name_4_condate STRING,
  previous_name_4_company_name STRING,
  previous_name_5_condate STRING,
  previous_name_5_company_name STRING,
  previous_name_6_condate STRING,
  previous_name_6_company_name STRING,
  previous_name_7_condate STRING,
  previous_name_7_company_name STRING,
  previous_name_8_condate STRING,
  previous_name_8_company_name STRING,
  previous_name_9_condate STRING,
  previous_name_9_company_name STRING,
  previous_name_10_condate STRING,
  previous_name_10_company_name STRING
)
COMMENT 'Companies registered at UK Companies House'
PARTITIONED BY (
  snapshot_year STRING,
  snapshot_month STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ESCAPED by '\\'
STORED AS SEQUENCEFILE
LOCATION '${hiveconf:company.data.location}';

MSCK REPAIR TABLE ${hiveconf:company.data.table};

SHOW TABLES ${hiveconf:company.data.table};

SHOW TBLPROPERTIES ${hiveconf:company.data.table};

SHOW PARTITIONS ${hiveconf:company.data.table};

--
-- Un-delimited table
--

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:company.data.table}_raw (
  company STRING
)
COMMENT 'Companies registered at UK Companies House (raw)'
PARTITIONED BY (
  snapshot_year STRING,
  snapshot_month STRING
)
STORED AS SEQUENCEFILE
LOCATION '${hiveconf:company.data.location}';

MSCK REPAIR TABLE ${hiveconf:company.data.table}_raw;

SHOW TABLES ${hiveconf:company.data.table}_raw;

SHOW TBLPROPERTIES ${hiveconf:company.data.table}_raw;

SHOW PARTITIONS ${hiveconf:company.data.table}_raw;


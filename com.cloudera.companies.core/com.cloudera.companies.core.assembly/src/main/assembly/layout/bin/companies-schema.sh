#!/bin/sh


export COMPANIES_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )""/.."

source "$COMPANIES_DIR/bin/companies.env"

set -x

hive --hiveconf "company.data.table=company" --hiveconf "company.data.location=$COMPANIES_EXPORT_DATA_SEQ_CLEANSED_DIR" -f "$COMPANIES_DIR/lib/query/com/cloudera/companies/core/query/ddl/create.sql"
hive --hiveconf "company.data.table=company_duplicate" --hiveconf "company.data.location=$COMPANIES_EXPORT_DATA_SEQ_DUPLICATE_DIR" -f "$COMPANIES_DIR/lib/query/com/cloudera/companies/core/query/ddl/create.sql"
hive --hiveconf "company.data.table=company_malformed" --hiveconf "company.data.location=$COMPANIES_EXPORT_DATA_SEQ_MALFORMED_DIR" -f "$COMPANIES_DIR/lib/query/com/cloudera/companies/core/query/ddl/create.sql"

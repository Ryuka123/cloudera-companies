#!/bin/sh


export COMPANIES_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )""/.."

source "$COMPANIES_DIR/bin/companies.env"

line_item_header "COUNT"
hive -f "$COMPANIES_DIR/lib/query/com/cloudera/companies/core/query/dml/count.sql"
line_item_header ""

line_item_header "SELECT"
hive -f "$COMPANIES_DIR/lib/query/com/cloudera/companies/core/query/dml/select.sql"
line_item_header ""

line_item_header "DUPLICATE"
hive -f "$COMPANIES_DIR/lib/query/com/cloudera/companies/core/query/dml/duplicate.sql"
line_item_header ""

line_item_header "MALFORMED"
hive -f "$COMPANIES_DIR/lib/query/com/cloudera/companies/core/query/dml/malformed.sql"
line_item_header ""

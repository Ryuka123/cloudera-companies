#!/bin/sh


export COMPANIES_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )""/.."

source "$COMPANIES_DIR/bin/companies.env"

function line_item_header() {
  echo "" && echo "-------------------------------------------------------------------------------" && echo "$1" && echo "-------------------------------------------------------------------------------" && echo "" && echo "" 
}

set -x

line_item_header "COUNT"
time hive -f "$COMPANIES_DIR/lib/query/com/cloudera/companies/core/query/dml/count.sql"
line_item_header ""

line_item_header "SELECT"
time hive -f "$COMPANIES_DIR/lib/query/com/cloudera/companies/core/query/dml/select.sql"
line_item_header ""

line_item_header "DUPLICATE"
time hive -f "$COMPANIES_DIR/lib/query/com/cloudera/companies/core/query/dml/duplicate.sql"
line_item_header ""

line_item_header "MALFORMED"
time hive -f "$COMPANIES_DIR/lib/query/com/cloudera/companies/core/query/dml/malformed.sql"
line_item_header ""

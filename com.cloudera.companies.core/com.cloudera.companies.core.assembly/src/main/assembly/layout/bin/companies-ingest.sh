#!/bin/sh

export COMPANIES_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )""/.."

source "$COMPANIES_DIR/bin/companies.env"

hadoop jar "$COMPANIES_DIR/lib/ingest/com.cloudera.companies.core.ingest-"*"-hadoop-job.jar" com.cloudera.companies.core.ingest.IngestDriver -D companies.ingest.thread.number=$COMPANIES_INGEST_THREADS "$COMPANIES_IMPORT_DATA_DIR" "$COMPANIES_EXPORT_DATA_ZIP_DIR" "$COMPANIES_EXPORT_DATA_SEQ_DIR"


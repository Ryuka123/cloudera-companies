#!/bin/sh

export COMPANIES_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )""/.."

source "$COMPANIES_DIR/bin/companies.env"

set -x

hadoop jar "$COMPANIES_DIR/lib/ingest/com.cloudera.companies.core.ingest-"*"-hadoop-job.jar" com.cloudera.companies.core.ingest.IngestFSCKDriver -D companies.ingest.fsck.clean=$COMPANIES_INGEST_FSCK_CLEAN "$COMPANIES_EXPORT_DATA_ZIP_DIR" "$COMPANIES_EXPORT_DATA_SEQ_DIR"


#!/bin/sh

source companies.env

hadoop jar "$COMPANIES_DIR/lib/ingest/com.cloudera.companies.core.ingest-*-hadoop-job.jar" com.cloudera.companies.core.ingest.IngestFSCKDriver "$COMPANIES_EXPORT_DATA_ZIP_DIR" "$COMPANIES_EXPORT_DATA_SEQ_DIR"


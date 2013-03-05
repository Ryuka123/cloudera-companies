package com.cloudera.companies.core.ingest.seq;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.cloudera.companies.core.common.CompaniesFileMetaData;
import com.cloudera.companies.core.ingest.IngestUtil.Counter;
import com.cloudera.companies.core.ingest.seq.mr.CompaniesFileKey;

public class IngestSeqMapper extends Mapper<Text, Text, CompaniesFileKey, Text> {

  @Override
  protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    context.getCounter(Counter.RECORDS).increment(1);
    String[] recordTokens = null;
    try {
      recordTokens = CompaniesFileMetaData.parseRecord(value.toString());
    } catch (IOException exception) {
      // ignore
    }
    if (recordTokens == null || recordTokens.length != CompaniesFileMetaData.FILE_FIELDS_NUMBER
        || recordTokens[0].length() == 0) {
      context.getCounter(Counter.RECORDS_MALFORMED).increment(1);
      context.write(new CompaniesFileKey(Counter.RECORDS_MALFORMED, key.toString(), recordTokens == null ? ""
          : recordTokens[0]), new Text(value));
    } else {
      context.getCounter(Counter.RECORDS_VALID).increment(1);
      context.write(new CompaniesFileKey(Counter.RECORDS_VALID, key.toString(), recordTokens[0]), new Text(value));
    }
  }

}
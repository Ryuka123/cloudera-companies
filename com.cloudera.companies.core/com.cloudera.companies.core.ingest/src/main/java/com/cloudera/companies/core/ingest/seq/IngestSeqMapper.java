package com.cloudera.companies.core.ingest.seq;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.cloudera.companies.core.common.CompaniesFileMetaData;
import com.cloudera.companies.core.common.mapreduce.CompaniesFileKey;
import com.cloudera.companies.core.ingest.IngestConstants.Counter;

public class IngestSeqMapper extends Mapper<Text, Text, CompaniesFileKey, Text> {

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String[] recordTokens = CompaniesFileMetaData.parseRecord(value.toString());
			if (recordTokens.length != CompaniesFileMetaData.FILE_FIELDS_NUMBER) {
				context.getCounter(Counter.RECORDS_MALFORMED).increment(1);
			} else if (recordTokens[0].length() == 0) {
				context.getCounter(Counter.RECORDS_MALFORMED_KEY).increment(1);
			} else {
				context.write(new CompaniesFileKey(key.toString(), recordTokens[0]), new Text(value));
				context.getCounter(Counter.RECORDS_VALID).increment(1);
			}
		} catch (IOException e) {
			context.getCounter(Counter.RECORDS_MALFORMED).increment(1);
		}
	}

}
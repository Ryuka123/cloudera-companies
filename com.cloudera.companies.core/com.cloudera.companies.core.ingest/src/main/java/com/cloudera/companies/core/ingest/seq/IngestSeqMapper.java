package com.cloudera.companies.core.ingest.seq;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.cloudera.companies.core.common.CompaniesFileMetaData;
import com.cloudera.companies.core.ingest.seq.IngestSeqDriver.RecordCounter;

public class IngestSeqMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String[] recordTokens = CompaniesFileMetaData.parseRecord(value.toString());
			if (recordTokens.length != CompaniesFileMetaData.FILE_FIELDS_NUMBER) {
				context.getCounter(RecordCounter.MALFORMED).increment(1);
			} else if (recordTokens[0].length() == 0) {
				context.getCounter(RecordCounter.MALFORMED_KEY).increment(1);
			} else {
				context.write(new Text(recordTokens[0]), new Text(value));
				context.getCounter(RecordCounter.VALID).increment(1);
			}
		} catch (IOException e) {
			context.getCounter(RecordCounter.MALFORMED).increment(1);
		}
	}

}
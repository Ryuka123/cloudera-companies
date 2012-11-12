package com.cloudera.companies.core.ingest.etl;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.cloudera.companies.core.ingest.etl.CompanyDataSetETLDriver.RecordCounter;

public class CompanyDataSetETLReducer extends Reducer<Text, Text, Text, Text> {

	protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException,
			InterruptedException {
		boolean recordProcessed = false;
		for (Text value : values) {
			if (!recordProcessed) {
				context.write(key, value);
			} else {
				context.getCounter(RecordCounter.MALFORMED_DUPLICATE).increment(1);
			}
		}
	}
}
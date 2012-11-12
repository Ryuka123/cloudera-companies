package com.cloudera.companies.core.ingest.etl;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.cloudera.companies.core.ingest.etl.CompanyDataSetETLDriver.RecordCounter;

public class CompanyDataSetETLMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String valueString = value.toString();
		int indexOfFirstDelim = valueString.indexOf(',');
		if (indexOfFirstDelim > 0 && indexOfFirstDelim < valueString.length()) {
			String companyName = valueString.substring(0, indexOfFirstDelim).trim();
			if (companyName.length() > 0) {
				String companyDetails = valueString.substring(indexOfFirstDelim + 1, valueString.length());
				context.write(new Text(companyName), new Text(companyDetails));
				context.getCounter(RecordCounter.VALID).increment(1);
			} else {
				context.getCounter(RecordCounter.MALFORMED_KEY).increment(1);
			}
		} else {
			context.getCounter(RecordCounter.MALFORMED).increment(1);
		}
	}

}
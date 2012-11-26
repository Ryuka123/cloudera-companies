package com.cloudera.companies.core.ingest.seq;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.cloudera.companies.core.common.CompaniesFileMetaData;
import com.cloudera.companies.core.common.mapreduce.CompaniesFileKey;
import com.cloudera.companies.core.ingest.IngestConstants.Counter;

public class IngestSeqReducer extends Reducer<CompaniesFileKey, Text, Text, Text> {

	private MultipleOutputs<Text, Text> multipleOutputs;

	@Override
	public void setup(Context context) {
		multipleOutputs = new MultipleOutputs<Text, Text>(context);
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}

	@Override
	protected void reduce(CompaniesFileKey key, Iterable<Text> values, Context context) throws java.io.IOException,
			InterruptedException {
		String lastName = null;
		for (Text value : values) {
			if (lastName == null || !lastName.equals(key.getName())) {
				lastName = key.getName();
				Text keyOutput = new Text(key.getGroup());
				Text valueOutput = new Text(value);
				try {
					multipleOutputs.write(IngestSeqDriver.NAMED_OUTPUT_PARTION_SEQ_FILES, keyOutput, valueOutput,
							CompaniesFileMetaData.parseGroup(key.getGroup()));
				} catch (IllegalArgumentException exception) {
					context.write(keyOutput, valueOutput);
				}
			} else {
				context.getCounter(Counter.RECORDS_PROCESSED_VALID).increment(-1);
				context.getCounter(Counter.RECORDS_PROCESSED_MALFORMED_DUPLICATE).increment(1);
			}
		}
	}
}
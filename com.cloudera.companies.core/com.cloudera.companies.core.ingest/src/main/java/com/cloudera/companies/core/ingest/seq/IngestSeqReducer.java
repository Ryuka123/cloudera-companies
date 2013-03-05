package com.cloudera.companies.core.ingest.seq;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.cloudera.companies.core.ingest.IngestUtil;
import com.cloudera.companies.core.ingest.IngestUtil.Counter;
import com.cloudera.companies.core.ingest.seq.mr.CompaniesFileKey;

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
      Text keyOutput = new Text(key.getName());
      Text valueOutput = new Text(value);
      try {
        if (lastName != null && !lastName.equals("") && lastName.equals(key.getName())) {
          context.getCounter(Counter.RECORDS_VALID).increment(-1);
          context.getCounter(Counter.RECORDS_DUPLICATE).increment(1);
          multipleOutputs.write(IngestSeqDriver.NAMED_OUTPUT_PARTION_SEQ_FILES, keyOutput, valueOutput,
              IngestUtil.getNamespacedPathFile(Counter.RECORDS_DUPLICATE, key.getGroup()));
        } else {
          lastName = key.getName();
          multipleOutputs.write(IngestSeqDriver.NAMED_OUTPUT_PARTION_SEQ_FILES, keyOutput, valueOutput,
              IngestUtil.getNamespacedPathFile(key.getType(), key.getGroup()));
        }
      } catch (IllegalArgumentException exception) {
        // necessary for MRUnit to work with MultipleOutputs
        context.write(keyOutput, valueOutput);
      }
    }
  }
}
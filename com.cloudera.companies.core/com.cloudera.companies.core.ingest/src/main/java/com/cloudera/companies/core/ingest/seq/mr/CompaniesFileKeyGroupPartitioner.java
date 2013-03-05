package com.cloudera.companies.core.ingest.seq.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CompaniesFileKeyGroupPartitioner extends Partitioner<CompaniesFileKey, Text> {

  @Override
  public int getPartition(CompaniesFileKey key, Text value, int numPartitions) {
    return (int) (((key.getGroup().hashCode()) & 0x00000000ffffffffL) % numPartitions);
  }

}

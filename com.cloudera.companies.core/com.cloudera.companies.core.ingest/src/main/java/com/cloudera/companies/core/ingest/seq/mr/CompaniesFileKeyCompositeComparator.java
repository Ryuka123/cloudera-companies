package com.cloudera.companies.core.ingest.seq.mr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompaniesFileKeyCompositeComparator extends WritableComparator {

  protected CompaniesFileKeyCompositeComparator() {
    super(CompaniesFileKey.class, true);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public int compare(WritableComparable one, WritableComparable two) {
    int result = ((CompaniesFileKey) one).getGroup().compareTo(((CompaniesFileKey) two).getGroup());
    return result == 0 ? -1 * ((CompaniesFileKey) one).getName().compareTo(((CompaniesFileKey) two).getName()) : result;
  }
}

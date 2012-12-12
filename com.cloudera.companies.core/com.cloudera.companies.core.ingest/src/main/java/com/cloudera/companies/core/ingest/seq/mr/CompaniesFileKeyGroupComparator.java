package com.cloudera.companies.core.ingest.seq.mr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompaniesFileKeyGroupComparator extends WritableComparator {

	protected CompaniesFileKeyGroupComparator() {
		super(CompaniesFileKey.class, true);
	}

	@Override
	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable one, WritableComparable two) {
		return ((CompaniesFileKey) one).getGroup().compareTo(((CompaniesFileKey) two).getGroup());
	}

}

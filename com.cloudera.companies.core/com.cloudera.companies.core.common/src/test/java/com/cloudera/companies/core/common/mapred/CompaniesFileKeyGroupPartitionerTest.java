package com.cloudera.companies.core.common.mapred;

import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.cloudera.companies.core.common.mapreduce.CompaniesFileKey;
import com.cloudera.companies.core.common.mapreduce.CompaniesFileKeyGroupPartitioner;
import com.cloudera.companies.core.test.CompaniesBaseTestCase;

public class CompaniesFileKeyGroupPartitionerTest extends CompaniesBaseTestCase {

	public static final int NUM_PARTITIONS = 10;

	private CompaniesFileKeyGroupPartitioner partitioner = new CompaniesFileKeyGroupPartitioner();

	@Test
	public void test1() {
		Assert.assertTrue(withinRange(partitioner.getPartition(
				new CompaniesFileKey("2012/12", "" + "Some Company Name"), new Text("Some CSV"), NUM_PARTITIONS), -1,
				NUM_PARTITIONS + 1));
	}

	private boolean withinRange(int value, int lower, int upper) {
		return value > lower && value < upper;
	}

}

package com.cloudera.companies.core.ingest.etl;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.cloudera.companies.core.ingest.etl.CompanyDataSetETLDriver.RecordCounter;

public class CompanyDataSetFileCopyTest {

	private MapDriver<Text, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<Text, Text, Text, Text, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {
		CompanyDataSetETLMapper mapper = new CompanyDataSetETLMapper();
		CompanyDataSetETLReducer reducer = new CompanyDataSetETLReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Ignore
	@Test
	public void testMapperValid() {
		mapDriver.withInput(new Text(), new Text(""));
		mapDriver.withOutput(new Text("6"), new Text("1"));
		mapDriver.runTest();
		Assert.assertEquals(1, mapDriver.getCounters().findCounter(RecordCounter.VALID).getValue());
	}

	@Ignore
	@Test
	public void testReducer() {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("1"));
		values.add(new Text("1"));
		reduceDriver.withInput(new Text("6"), values);
		reduceDriver.withOutput(new Text("6"), new Text("2"));
		reduceDriver.runTest();
	}

}

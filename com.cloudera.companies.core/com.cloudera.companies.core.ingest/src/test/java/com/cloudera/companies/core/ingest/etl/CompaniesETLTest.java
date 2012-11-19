package com.cloudera.companies.core.ingest.etl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Assert;

import com.cloudera.companies.core.ingest.etl.CompaniesETLDriver.RecordCounter;
import com.cloudera.companies.core.test.CompaniesCDHTestCase;

public class CompaniesETLTest extends CompaniesCDHTestCase {

	private MapDriver<Text, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<Text, Text, Text, Text, Text, Text> mapReduceDriver;

	public CompaniesETLTest() throws IOException {
	}

	@Override
	public void setUp() {
		CompaniesETLMapper mapper = new CompaniesETLMapper();
		CompaniesETLReducer reducer = new CompaniesETLReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	public void testMapperValid() {
		String record = "\"Company X\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\"";
		mapDriver.withInput(new Text(), new Text(record));
		mapDriver.withOutput(new Text("Company X"), new Text(record));
		mapDriver.runTest();
		Assert.assertEquals(1, mapDriver.getCounters().findCounter(RecordCounter.VALID).getValue());
	}

	public void testReducer() {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("\"Company X\""));
		reduceDriver.withInput(new Text("Company X"), values);
		reduceDriver.withOutput(new Text("Company X"), new Text("\"Company X\""));
		reduceDriver.runTest();
	}

	public void testMapReduce() {
		String record = "\"Company X\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\"";
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("\"Company X\""));
		mapReduceDriver.withInput(new Text(""), new Text(record));
		mapReduceDriver.withOutput(new Text("Company X"), new Text(record));
		mapReduceDriver.runTest();
	}

}

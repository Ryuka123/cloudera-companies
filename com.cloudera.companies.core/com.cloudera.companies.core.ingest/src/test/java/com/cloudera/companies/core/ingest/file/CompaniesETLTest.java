package com.cloudera.companies.core.ingest.file;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import com.cloudera.companies.core.common.CompaniesCDHTestCase;
import com.cloudera.companies.core.ingest.etl.CompaniesETLMapper;
import com.cloudera.companies.core.ingest.etl.CompaniesETLReducer;

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
		// mapDriver.withInput(new Text(), new Text(""));
		// mapDriver.withOutput(new Text("6"), new Text("1"));
		// mapDriver.runTest();
		// Assert.assertEquals(1,
		// mapDriver.getCounters().findCounter(RecordCounter.VALID).getValue());
	}

	public void testReducer() {
		// List<Text> values = new ArrayList<Text>();
		// values.add(new Text("1"));
		// values.add(new Text("1"));
		// reduceDriver.withInput(new Text("6"), values);
		// reduceDriver.withOutput(new Text("6"), new Text("2"));
		// reduceDriver.runTest();
	}
}

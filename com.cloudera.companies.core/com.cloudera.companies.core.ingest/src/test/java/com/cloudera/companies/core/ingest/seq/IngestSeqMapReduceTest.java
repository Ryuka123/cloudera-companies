package com.cloudera.companies.core.ingest.seq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Assert;

import com.cloudera.companies.core.ingest.seq.IngestSeqMapper;
import com.cloudera.companies.core.ingest.seq.IngestSeqReducer;
import com.cloudera.companies.core.ingest.seq.IngestSeqDriver.RecordCounter;
import com.cloudera.companies.core.test.CompaniesCDHTestCase;

public class IngestSeqMapReduceTest extends CompaniesCDHTestCase {

	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

	public IngestSeqMapReduceTest() throws IOException {
	}

	@Override
	public void setUp() {
		IngestSeqMapper mapper = new IngestSeqMapper();
		IngestSeqReducer reducer = new IngestSeqReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	public void testMapperValid() {
		String record = "\"Company X\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\"";
		mapDriver.withInput(new LongWritable(0), new Text(record));
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
		mapReduceDriver.withInput(new LongWritable(0), new Text(record));
		mapReduceDriver.withOutput(new Text("Company X"), new Text(record));
		mapReduceDriver.runTest();
	}

}

package com.cloudera.companies.core.ingest.seq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.companies.core.ingest.IngestUtil.Counter;
import com.cloudera.companies.core.ingest.seq.mr.CompaniesFileKey;
import com.cloudera.companies.core.test.CompaniesBaseTestCase;

public class IngestSeqMapReduceTest extends CompaniesBaseTestCase {

  private MapDriver<Text, Text, CompaniesFileKey, Text> mapDriver;
  private ReduceDriver<CompaniesFileKey, Text, Text, Text> reduceDriver;
  private MapReduceDriver<Text, Text, CompaniesFileKey, Text, Text, Text> mapReduceDriver;

  private static final String INPUT_GROUP = "2012/01";
  private static final String INPUT_NAME = "Company X";
  private static final Text INPUT_GROUP_TEXT = new Text(INPUT_GROUP);
  private static final Text INPUT_NAME_TEXT = new Text(INPUT_NAME);
  private static final CompaniesFileKey INPUT_KEY = new CompaniesFileKey(Counter.RECORDS_VALID, INPUT_GROUP, INPUT_NAME);
  private static final String INPUT_RECORD = "\""
      + INPUT_NAME
      + "\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\"";
  private static final Text INPUT_RECORD_TEXT = new Text(INPUT_RECORD);

  public IngestSeqMapReduceTest() throws IOException {
  }

  @Before
  public void setUp() {
    IngestSeqMapper mapper = new IngestSeqMapper();
    IngestSeqReducer reducer = new IngestSeqReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }

  @Test
  public void testMapperValid() {
    mapDriver.withInput(INPUT_GROUP_TEXT, INPUT_RECORD_TEXT);
    mapDriver.withOutput(INPUT_KEY, INPUT_RECORD_TEXT);
    mapDriver.runTest();
    Assert.assertEquals(1, mapDriver.getCounters().findCounter(Counter.RECORDS_VALID).getValue());
  }

  @Test
  public void testReducer() {
    List<Text> values = new ArrayList<Text>();
    values.add(INPUT_RECORD_TEXT);
    reduceDriver.withInput(INPUT_KEY, values);
    reduceDriver.withOutput(INPUT_NAME_TEXT, INPUT_RECORD_TEXT);
    reduceDriver.runTest();
  }

  @Test
  public void testMapReduce() {
    mapReduceDriver.withInput(INPUT_GROUP_TEXT, INPUT_RECORD_TEXT);
    mapReduceDriver.withOutput(INPUT_NAME_TEXT, INPUT_RECORD_TEXT);
    mapReduceDriver.runTest();
  }

}

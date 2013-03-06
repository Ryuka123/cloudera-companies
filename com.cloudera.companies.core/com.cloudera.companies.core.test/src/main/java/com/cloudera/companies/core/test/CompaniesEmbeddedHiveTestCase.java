package com.cloudera.companies.core.test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.service.HiveInterface;
import org.apache.hadoop.hive.service.HiveServer;
import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;

public abstract class CompaniesEmbeddedHiveTestCase extends CompaniesEmbeddedCoreTestCase {

  private HiveInterface hive;

  public CompaniesEmbeddedHiveTestCase() throws IOException {
    super();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    hive = new HiveServer.HiveServerHandler();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    hive.shutdown();
    super.tearDown();
  }

  public void execute(String query) throws HiveServerException, TException {
    hive.execute(query);
  }

  public List<String> executeAndFetch(String query) throws HiveServerException, TException {
    execute(query);
    return hive.fetchAll();
  }

  public String executeAndFetchOne(String query) throws HiveServerException, TException {
    execute(query);
    return hive.fetchOne();
  }

  public void execute(String directory, String file) throws HiveServerException, TException, IOException {
    for (String query : readColonDelimiteredLinesFromFileOnClasspath(directory, file)) {
      hive.execute(query);
    }
  }

  public List<String> executeAndFetch(String directory, String file) throws HiveServerException, TException,
      IOException {
    execute(directory, file);
    return hive.fetchAll();
  }

  public String executeAndFetchOne(String directory, String file) throws HiveServerException, TException, IOException {
    execute(directory, file);
    return hive.fetchOne();
  }

  private List<String> readColonDelimiteredLinesFromFileOnClasspath(String directory, String file) throws IOException {
    List<String> lines = new ArrayList<String>();
    URL fileUrl = CompaniesEmbeddedHiveTestCase.class.getResource(directory + "/" + file);
    if (fileUrl != null) {
      for (String line : FileUtils.readFileToString(new File(fileUrl.getFile())).split(";")) {
        if (!line.trim().equals("")) {
          lines.add(line.trim());
        }
      }
      return lines;
    }
    throw new IOException("Could not load file [" + directory + "/" + file + "] from classpath");
  }
}

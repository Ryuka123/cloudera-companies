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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CompaniesEmbeddedHiveTestCase extends CompaniesEmbeddedCoreTestCase {

  private static Logger log = LoggerFactory.getLogger(CompaniesEmbeddedHiveTestCase.class);

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
    _execute(query);
  }

  public List<String> executeAndFetchAll(String query) throws HiveServerException, TException {
    execute(query);
    return _fetchAll(query);
  }

  public String executeAndFetchOne(String query) throws HiveServerException, TException {
    execute(query);
    return _fetchOne(query);
  }

  public String execute(String directory, String file) throws HiveServerException, TException, IOException {
    String lastQuery = null;
    for (String query : readColonDelimiteredLinesFromFileOnClasspath(directory, file)) {
      hive.execute(lastQuery = query);
    }
    return lastQuery;
  }

  public List<String> executeAndFetchAll(String directory, String file) throws HiveServerException, TException,
      IOException {
    return _fetchAll(execute(directory, file));
  }

  public String executeAndFetchOne(String directory, String file) throws HiveServerException, TException, IOException {
    return _fetchOne(execute(directory, file));
  }

  private void _execute(String query) throws HiveServerException, TException {
    if (log.isDebugEnabled()) {
      log.debug("Hive client test pre-execute:\n" + query + "\n");
    }
    hive.execute(query);
    if (log.isDebugEnabled()) {
      log.debug("Hive client test post-execute:\n" + query + "\n");
    }
  }

  private List<String> _fetchAll(String query) throws HiveServerException, TException {
    List<String> rows = hive.fetchAll();
    if (log.isDebugEnabled()) {
      StringBuilder rowsString = new StringBuilder();
      rowsString.append("Hive client test fetched results:\n" + query + "\n");
      for (String row : rows) {
        rowsString.append('\n');
        rowsString.append(row);
      }
      rowsString.append('\n');
      log.debug(rowsString.toString());
    }
    return rows;
  }

  private String _fetchOne(String query) throws HiveServerException, TException {
    String row = hive.fetchOne();
    if (log.isDebugEnabled()) {
      StringBuilder rowsString = new StringBuilder();
      rowsString.append("Hive client test fetched results:\n" + query + "\n");
      rowsString.append('\n');
      rowsString.append(row);
      rowsString.append('\n');
      log.debug(rowsString.toString());
    }
    return row;

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

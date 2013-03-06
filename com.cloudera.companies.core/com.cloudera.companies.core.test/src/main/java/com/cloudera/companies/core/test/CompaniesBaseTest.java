package com.cloudera.companies.core.test;

import java.io.File;

public interface CompaniesBaseTest {

  public static String ENV_HADOOP_HOME = "HADOOP_HOME";

  public static String PATH_HADOOP_HOME = "target/test-runtime/hadoop";

  public static String PATH_HDFS = "target/test-hdfs";
  public static String PATH_LOCAL = "target/test-local";

  public static String PATH_LOCAL_WORKING_DIR = new File(".").getAbsolutePath();
  public static String PATH_LOCAL_WORKING_DIR_TARGET = PATH_LOCAL_WORKING_DIR + "/target";
  public static String PATH_LOCAL_WORKING_DIR_TARGET_HDFS = PATH_LOCAL_WORKING_DIR_TARGET + "/test-hdfs";

  public static final String PATH_LOCAL_INPUT_DIR_ZIP = CompaniesBaseTestCase
      .getPathLocal("target/test-data/data/basiccompany/sample/zip");
  public static final String PATH_HDFS_OUTPUT_DIR_ZIP = CompaniesBaseTestCase
      .getPathHDFS("test-ouput/companies/original");
  public static final String PATH_HDFS_OUTPUT_DIR_SEQ = CompaniesBaseTestCase
      .getPathHDFS("test-ouput/companies/processed");

}

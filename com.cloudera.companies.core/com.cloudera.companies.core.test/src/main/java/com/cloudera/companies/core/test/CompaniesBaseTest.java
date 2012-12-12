package com.cloudera.companies.core.test;

import java.io.File;

public interface CompaniesBaseTest {

	public static String PATH_LOCAL_WORKING_DIR = new File(".").getAbsolutePath();

	public static final String PATH_LOCAL_INPUT_DIR_ZIP = CompaniesBaseTestCase
			.getPathLocal("target/test-data/data/basiccompany/sample/zip");
	public static final String PATH_HDFS_OUTPUT_DIR_ZIP = CompaniesBaseTestCase.getPathHDFS("test-ouput/companies/original");
	public static final String PATH_HDFS_OUTPUT_DIR_SEQ = CompaniesBaseTestCase.getPathHDFS("test-ouput/companies/processed");

}

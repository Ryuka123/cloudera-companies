package com.cloudera.companies.core.ingest.etl;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;

import com.cloudera.companies.core.common.CompaniesDriver;
import com.cloudera.companies.core.test.CompaniesCDHTestCase;

public class CompaniesETLDriverTest extends CompaniesCDHTestCase {

	private CompaniesETLDriver companiesETLDriver;

	public CompaniesETLDriverTest() throws IOException {
		super();
	}

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		companiesETLDriver = new CompaniesETLDriver();
		companiesETLDriver.setConf(getFileSystem().getConf());
	}

	public void testLaunchInvalid() throws Exception {

		String inputDir = getPathHDFS("/test-input");
		String inputFile = getPathHDFS("/test-input/input.txt");
		String inputNonExistantDir = getPathHDFS("/test-input-does-not-exist");

		String outputDir = getPathHDFS("/test-output");
		String outputFile = getPathHDFS("/test-output/output.txt");

		new File(inputDir).mkdirs();
		new File(inputFile).createNewFile();
		new File(outputDir).mkdirs();
		new File(outputFile).createNewFile();

		try {
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_MISSING_ARGS, companiesETLDriver.run(null));
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_MISSING_ARGS, companiesETLDriver.run(new String[0]));
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_MISSING_ARGS, companiesETLDriver.run(new String[] {}));
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_MISSING_ARGS,
					companiesETLDriver.run(new String[] { getPathLocal("") }));
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_MISSING_ARGS,
					companiesETLDriver.run(new String[] { inputDir }));
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
					companiesETLDriver.run(new String[] { getPathLocal(inputFile), outputDir }));
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
					companiesETLDriver.run(new String[] { getPathLocal(inputNonExistantDir), outputDir }));
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
					companiesETLDriver.run(new String[] { inputDir, outputFile }));

			if (!UserGroupInformation.getCurrentUser().getUserName().equals("root")) {

				getFileSystem().setPermission(new Path(inputDir),
						new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						companiesETLDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(inputDir),
						new FsPermission(FsAction.READ, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						companiesETLDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(inputDir),
						new FsPermission(FsAction.WRITE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						companiesETLDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(inputDir),
						new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						companiesETLDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(inputDir),
						new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
				getFileSystem().setPermission(new Path(outputDir),
						new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						companiesETLDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(outputDir),
						new FsPermission(FsAction.READ, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						companiesETLDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(outputDir),
						new FsPermission(FsAction.WRITE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						companiesETLDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(outputDir),
						new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						companiesETLDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(outputDir),
						new FsPermission(FsAction.EXECUTE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						companiesETLDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(outputDir),
						new FsPermission(FsAction.READ_EXECUTE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						companiesETLDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(outputDir),
						new FsPermission(FsAction.WRITE_EXECUTE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						companiesETLDriver.run(new String[] { inputDir, outputDir }));

			}
		} finally {
			getFileSystem().setPermission(new Path(inputDir),
					new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
			getFileSystem().setPermission(new Path(outputDir),
					new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
		}
	}

	public void testFileCopySingleThread() throws Exception {

		String inputDir = getPathLocal("/target/test-data/data/basiccompany/sample/zip");
		String outputDir = getPathHDFS("/test-output");

		Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS,
				companiesETLDriver.run(new String[] { inputDir, outputDir }));
	}

}

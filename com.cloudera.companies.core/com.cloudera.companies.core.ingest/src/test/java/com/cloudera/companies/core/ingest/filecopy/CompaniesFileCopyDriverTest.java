package com.cloudera.companies.core.ingest.filecopy;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;

import com.cloudera.companies.core.common.CompaniesDriver;
import com.cloudera.companies.core.test.CompaniesCDHTestCase;

public class CompaniesFileCopyDriverTest extends CompaniesCDHTestCase {

	private CompaniesFileCopyDriver companiesFileCopyDriver;

	public CompaniesFileCopyDriverTest() throws IOException {
		super();
	}

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		companiesFileCopyDriver = new CompaniesFileCopyDriver();
		companiesFileCopyDriver.setConf(getFileSystem().getConf());
	}

	public void testLaunchInvalid() throws Exception {

		String inputDir = getPathLocal("/target/test-data");
		String inputFile = getPathLocal("/target/test-data/input.txt");
		String inputNonExistantDir = getPathLocal("/target/test-data-does-not-exist");

		String outputDir = getPathHDFS("/test-output");
		String outputFile = getPathHDFS("/test-output/output.txt");

		new File(inputDir).mkdirs();
		new File(inputFile).createNewFile();
		new File(outputDir).mkdirs();
		new File(outputFile).createNewFile();

		try {
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_MISSING_ARGS, companiesFileCopyDriver.run(null));
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_MISSING_ARGS, companiesFileCopyDriver.run(new String[0]));
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_MISSING_ARGS,
					companiesFileCopyDriver.run(new String[] {}));
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_MISSING_ARGS,
					companiesFileCopyDriver.run(new String[] { getPathLocal("") }));
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_MISSING_ARGS,
					companiesFileCopyDriver.run(new String[] { inputDir }));
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
					companiesFileCopyDriver.run(new String[] { getPathLocal(inputFile), outputDir }));
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
					companiesFileCopyDriver.run(new String[] { getPathLocal(inputNonExistantDir), outputDir }));
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
					companiesFileCopyDriver.run(new String[] { inputDir, outputFile }));

			if (!UserGroupInformation.getCurrentUser().getUserName().equals("root")) {

				getFileSystem().setPermission(new Path(inputDir),
						new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						companiesFileCopyDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(inputDir),
						new FsPermission(FsAction.READ, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						companiesFileCopyDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(inputDir),
						new FsPermission(FsAction.WRITE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						companiesFileCopyDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(inputDir),
						new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						companiesFileCopyDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(inputDir),
						new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
				getFileSystem().setPermission(new Path(outputDir),
						new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						companiesFileCopyDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(outputDir),
						new FsPermission(FsAction.READ, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						companiesFileCopyDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(outputDir),
						new FsPermission(FsAction.WRITE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						companiesFileCopyDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(outputDir),
						new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						companiesFileCopyDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(outputDir),
						new FsPermission(FsAction.EXECUTE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						companiesFileCopyDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(outputDir),
						new FsPermission(FsAction.READ_EXECUTE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						companiesFileCopyDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(outputDir),
						new FsPermission(FsAction.WRITE_EXECUTE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						companiesFileCopyDriver.run(new String[] { inputDir, outputDir }));

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
				companiesFileCopyDriver.run(new String[] { inputDir, outputDir }));
		Assert.assertEquals(1, getFileSystem().listStatus(new Path(outputDir)).length);
		for (FileStatus yearFileStatus : getFileSystem().listStatus(new Path(outputDir))) {
			Assert.assertEquals(2, getFileSystem().listStatus(yearFileStatus.getPath()).length);
			for (FileStatus monthFileStatus : getFileSystem().listStatus(yearFileStatus.getPath())) {
				Assert.assertEquals(5, getFileSystem().listStatus(monthFileStatus.getPath()).length);
				Assert.assertTrue(getFileSystem().exists(
						new Path(monthFileStatus.getPath(),
								CompaniesDriver.CONF_MR_FILECOMMITTER_SUCCEEDED_FILE_NAME)));
			}
		}
	}

	public void testFileCopyMultiThread() throws Exception {
		getFileSystem().getConf().set(CompaniesFileCopyDriver.CONF_THREAD_NUMBER, "10");
		try {
			testFileCopySingleThread();
		} finally {
			getFileSystem().getConf().set(CompaniesFileCopyDriver.CONF_THREAD_NUMBER, "1");
		}
	}

}

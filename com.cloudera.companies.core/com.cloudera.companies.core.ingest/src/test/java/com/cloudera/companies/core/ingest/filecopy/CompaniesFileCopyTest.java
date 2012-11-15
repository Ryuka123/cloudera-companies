package com.cloudera.companies.core.ingest.filecopy;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import com.cloudera.companies.core.test.CompaniesCDHTestCase;

public class CompaniesFileCopyTest extends CompaniesCDHTestCase {

	private CompaniesFileCopyDriver companiesFileCopyDriver;

	public CompaniesFileCopyTest() throws IOException {
		super();
	}

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		companiesFileCopyDriver = new CompaniesFileCopyDriver();
		companiesFileCopyDriver.setConf(getFileSystem().getConf());
	}

	public void testLaunchInvalid() throws Exception {

		String inputDir = getPathLocal("/target/test-input");
		String inputFile = getPathLocal("/target/test-input/input.txt");
		String inputNonExistantDir = getPathLocal("/target/test-input-does-not-exist");

		String outputDir = getPathHDFS("/test-output");
		String outputFile = getPathHDFS("/test-output/output.txt");

		new File(inputDir).mkdirs();
		new File(inputFile).createNewFile();
		new File(outputDir).mkdirs();
		new File(outputFile).createNewFile();

		try {
			Assert.assertEquals(CompaniesFileCopyDriver.RETURN_FAILURE_MISSING_ARGS, companiesFileCopyDriver.run(null));
			Assert.assertEquals(CompaniesFileCopyDriver.RETURN_FAILURE_MISSING_ARGS,
					companiesFileCopyDriver.run(new String[0]));
			Assert.assertEquals(CompaniesFileCopyDriver.RETURN_FAILURE_MISSING_ARGS,
					companiesFileCopyDriver.run(new String[] {}));
			Assert.assertEquals(CompaniesFileCopyDriver.RETURN_FAILURE_MISSING_ARGS,
					companiesFileCopyDriver.run(new String[] { getPathLocal("") }));
			Assert.assertEquals(CompaniesFileCopyDriver.RETURN_FAILURE_MISSING_ARGS,
					companiesFileCopyDriver.run(new String[] { inputDir }));
			Assert.assertEquals(CompaniesFileCopyDriver.RETURN_FAILURE_INVALID_ARGS,
					companiesFileCopyDriver.run(new String[] { getPathLocal(inputFile), outputDir }));
			Assert.assertEquals(CompaniesFileCopyDriver.RETURN_FAILURE_INVALID_ARGS,
					companiesFileCopyDriver.run(new String[] { getPathLocal(inputNonExistantDir), outputDir }));
			Assert.assertEquals(CompaniesFileCopyDriver.RETURN_FAILURE_INVALID_ARGS,
					companiesFileCopyDriver.run(new String[] { inputDir, outputFile }));

			getFileSystem().setPermission(new Path(inputDir),
					new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.NONE));

			Assert.assertEquals(CompaniesFileCopyDriver.RETURN_FAILURE_INVALID_ARGS,
					companiesFileCopyDriver.run(new String[] { inputDir, outputDir }));

			getFileSystem().setPermission(new Path(inputDir),
					new FsPermission(FsAction.READ, FsAction.NONE, FsAction.NONE));

			Assert.assertEquals(CompaniesFileCopyDriver.RETURN_FAILURE_INVALID_ARGS,
					companiesFileCopyDriver.run(new String[] { inputDir, outputDir }));

			getFileSystem().setPermission(new Path(inputDir),
					new FsPermission(FsAction.WRITE, FsAction.NONE, FsAction.NONE));

			Assert.assertEquals(CompaniesFileCopyDriver.RETURN_FAILURE_INVALID_ARGS,
					companiesFileCopyDriver.run(new String[] { inputDir, outputDir }));

			getFileSystem().setPermission(new Path(inputDir),
					new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE));

			Assert.assertEquals(CompaniesFileCopyDriver.RETURN_FAILURE_INVALID_ARGS,
					companiesFileCopyDriver.run(new String[] { inputDir, outputDir }));

			getFileSystem().setPermission(new Path(inputDir),
					new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
			getFileSystem().setPermission(new Path(outputDir),
					new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.NONE));

			Assert.assertEquals(CompaniesFileCopyDriver.RETURN_FAILURE_INVALID_ARGS,
					companiesFileCopyDriver.run(new String[] { inputDir, outputDir }));

			getFileSystem().setPermission(new Path(outputDir),
					new FsPermission(FsAction.READ, FsAction.NONE, FsAction.NONE));

			Assert.assertEquals(CompaniesFileCopyDriver.RETURN_FAILURE_INVALID_ARGS,
					companiesFileCopyDriver.run(new String[] { inputDir, outputDir }));

			getFileSystem().setPermission(new Path(outputDir),
					new FsPermission(FsAction.WRITE, FsAction.NONE, FsAction.NONE));

			Assert.assertEquals(CompaniesFileCopyDriver.RETURN_FAILURE_INVALID_ARGS,
					companiesFileCopyDriver.run(new String[] { inputDir, outputDir }));

			getFileSystem().setPermission(new Path(outputDir),
					new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE));

			Assert.assertEquals(CompaniesFileCopyDriver.RETURN_FAILURE_INVALID_ARGS,
					companiesFileCopyDriver.run(new String[] { inputDir, outputDir }));

			getFileSystem().setPermission(new Path(outputDir),
					new FsPermission(FsAction.EXECUTE, FsAction.NONE, FsAction.NONE));

			Assert.assertEquals(CompaniesFileCopyDriver.RETURN_FAILURE_INVALID_ARGS,
					companiesFileCopyDriver.run(new String[] { inputDir, outputDir }));

			getFileSystem().setPermission(new Path(outputDir),
					new FsPermission(FsAction.READ_EXECUTE, FsAction.NONE, FsAction.NONE));

			Assert.assertEquals(CompaniesFileCopyDriver.RETURN_FAILURE_INVALID_ARGS,
					companiesFileCopyDriver.run(new String[] { inputDir, outputDir }));

			getFileSystem().setPermission(new Path(outputDir),
					new FsPermission(FsAction.WRITE_EXECUTE, FsAction.NONE, FsAction.NONE));

			Assert.assertEquals(CompaniesFileCopyDriver.RETURN_FAILURE_INVALID_ARGS,
					companiesFileCopyDriver.run(new String[] { inputDir, outputDir }));
		} finally {
			getFileSystem().setPermission(new Path(inputDir),
					new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
			getFileSystem().setPermission(new Path(outputDir),
					new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
		}
	}

	public void testFileCopy() throws Exception {

		String inputDir = getPathLocal("/target/test-input");
		String outputDir = getPathHDFS("/test-output");

		Assert.assertEquals(CompaniesFileCopyDriver.RETURN_SUCCESS,
				companiesFileCopyDriver.run(new String[] { inputDir, outputDir }));

	}
}

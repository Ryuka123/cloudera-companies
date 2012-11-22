package com.cloudera.companies.core.ingest.seq;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;

import com.cloudera.companies.core.common.CompaniesDriver;
import com.cloudera.companies.core.ingest.zip.IngestZipDriver;
import com.cloudera.companies.core.test.CompaniesCDHTestCase;

public class IngestSeqDriverTest extends CompaniesCDHTestCase {

	private IngestZipDriver ingestZipDriver;
	private IngestSeqDriver ingestSeqDriver;

	public IngestSeqDriverTest() throws IOException {
		super();
	}

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		ingestZipDriver = new IngestZipDriver();
		ingestZipDriver.setConf(getFileSystem().getConf());
		ingestSeqDriver = new IngestSeqDriver();
		ingestSeqDriver.setConf(getFileSystem().getConf());
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
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_MISSING_ARGS, ingestSeqDriver.run(null));
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_MISSING_ARGS, ingestSeqDriver.run(new String[0]));
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_MISSING_ARGS, ingestSeqDriver.run(new String[] {}));
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_MISSING_ARGS,
					ingestSeqDriver.run(new String[] { getPathLocal("") }));
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_MISSING_ARGS,
					ingestSeqDriver.run(new String[] { inputDir }));
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
					ingestSeqDriver.run(new String[] { getPathLocal(inputFile), outputDir }));
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
					ingestSeqDriver.run(new String[] { getPathLocal(inputNonExistantDir), outputDir }));
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
					ingestSeqDriver.run(new String[] { inputDir, outputFile }));

			if (!UserGroupInformation.getCurrentUser().getUserName().equals("root")) {

				getFileSystem().setPermission(new Path(inputDir),
						new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						ingestSeqDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(inputDir),
						new FsPermission(FsAction.READ, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						ingestSeqDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(inputDir),
						new FsPermission(FsAction.WRITE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						ingestSeqDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(inputDir),
						new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						ingestSeqDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(inputDir),
						new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
				getFileSystem().setPermission(new Path(outputDir),
						new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						ingestSeqDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(outputDir),
						new FsPermission(FsAction.READ, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						ingestSeqDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(outputDir),
						new FsPermission(FsAction.WRITE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						ingestSeqDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(outputDir),
						new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						ingestSeqDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(outputDir),
						new FsPermission(FsAction.EXECUTE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						ingestSeqDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(outputDir),
						new FsPermission(FsAction.READ_EXECUTE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						ingestSeqDriver.run(new String[] { inputDir, outputDir }));

				getFileSystem().setPermission(new Path(outputDir),
						new FsPermission(FsAction.WRITE_EXECUTE, FsAction.NONE, FsAction.NONE));

				Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
						ingestSeqDriver.run(new String[] { inputDir, outputDir }));

			}
		} finally {
			getFileSystem().setPermission(new Path(inputDir),
					new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
			getFileSystem().setPermission(new Path(outputDir),
					new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
		}
	}

	public void test() throws Exception {

		String inputDirZip = getPathLocal("/target/test-data/data/basiccompany/sample/zip");
		String outputDirZip = getPathHDFS("/test-output/zip");
		String outputDirSeq = getPathHDFS("/test-output/seq");

		Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS,
				ingestZipDriver.run(new String[] { inputDirZip, outputDirZip }));

		Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS,
				ingestSeqDriver.run(new String[] { outputDirZip, outputDirSeq }));
	}

}

package com.cloudera.companies.core.ingest.seq;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.companies.core.common.CompaniesDriver;
import com.cloudera.companies.core.ingest.IngestFSCKDriver;
import com.cloudera.companies.core.ingest.zip.IngestZipDriver;
import com.cloudera.companies.core.test.CompaniesBaseTestCase;
import com.cloudera.companies.core.test.CompaniesEmbeddedTestCase;

public class IngestSeqDriverTest extends CompaniesEmbeddedTestCase {

	private IngestZipDriver ingestZipDriver;
	private IngestSeqDriver ingestSeqDriver;
	private IngestFSCKDriver ingestFSCKDriver;

	public IngestSeqDriverTest() throws IOException {
		super();
	}

	@Override
	@Before
	public void setUp() throws Exception {
		super.setUp();
		ingestZipDriver = new IngestZipDriver(getFileSystem().getConf());
		ingestSeqDriver = new IngestSeqDriver(getFileSystem().getConf());
		ingestFSCKDriver = new IngestFSCKDriver(getFileSystem().getConf());
	}

	@Test
	public void testLaunchInvalid() throws Exception {

		String inputDir = CompaniesBaseTestCase.getPathHDFS("/test-input");
		String inputFile = CompaniesBaseTestCase.getPathHDFS("/test-input/input.txt");
		String inputNonExistantDir = CompaniesBaseTestCase.getPathHDFS("/test-input-does-not-exist");

		String outputDir = CompaniesBaseTestCase.getPathHDFS("/test-output");
		String outputFile = CompaniesBaseTestCase.getPathHDFS("/test-output/output.txt");

		new File(inputDir).mkdirs();
		new File(inputFile).createNewFile();
		new File(outputDir).mkdirs();
		new File(outputFile).createNewFile();

		try {
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_MISSING_ARGS, ingestSeqDriver.run(null));
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_MISSING_ARGS, ingestSeqDriver.run(new String[0]));
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_MISSING_ARGS, ingestSeqDriver.run(new String[] {}));
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_MISSING_ARGS,
					ingestSeqDriver.run(new String[] { CompaniesBaseTestCase.getPathLocal("") }));
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_MISSING_ARGS,
					ingestSeqDriver.run(new String[] { inputDir }));
			Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
					ingestSeqDriver.run(new String[] { CompaniesBaseTestCase.getPathLocal(inputFile), outputDir }));
			Assert.assertEquals(
					CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
					ingestSeqDriver.run(new String[] { CompaniesBaseTestCase.getPathLocal(inputNonExistantDir),
							outputDir }));

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

	@Test
	public void test() throws Exception {
		Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS,
				ingestZipDriver.run(new String[] { PATH_LOCAL_INPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_ZIP }));
		Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS,
				ingestSeqDriver.run(new String[] { PATH_HDFS_OUTPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_SEQ }));
		Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS,
				ingestFSCKDriver.run(new String[] { PATH_HDFS_OUTPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_SEQ }));
		Assert.assertEquals(true, ingestFSCKDriver.testIntegretity(2, 8).isEmpty());
	}

}

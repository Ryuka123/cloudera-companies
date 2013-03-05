package com.cloudera.companies.core.ingest.zip;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.companies.core.common.CompaniesDriver;
import com.cloudera.companies.core.test.CompaniesBaseTestCase;
import com.cloudera.companies.core.test.CompaniesEmbeddedCoreTestCase;

public class IngestZipDriverTest extends CompaniesEmbeddedCoreTestCase {

  private IngestZipDriver ingestZipDriver;

  public IngestZipDriverTest() throws IOException {
    super();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    ingestZipDriver = new IngestZipDriver(getFileSystem().getConf());
  }

  @Test
  public void testLaunchInvalid() throws Exception {

    String inputDir = CompaniesBaseTestCase.getPathLocal("/target/test-data");
    String inputFile = CompaniesBaseTestCase.getPathLocal("/target/test-data/input.txt");
    String inputNonExistantDir = CompaniesBaseTestCase.getPathLocal("/target/test-data-does-not-exist");

    String outputDir = CompaniesBaseTestCase.getPathHDFS("/test-output");
    String outputFile = CompaniesBaseTestCase.getPathHDFS("/test-output/output.txt");

    new File(inputDir).mkdirs();
    new File(inputFile).createNewFile();
    new File(outputDir).mkdirs();
    new File(outputFile).createNewFile();

    try {
      Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_MISSING_ARGS, ingestZipDriver.run(null));
      Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_MISSING_ARGS, ingestZipDriver.run(new String[0]));
      Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_MISSING_ARGS, ingestZipDriver.run(new String[] {}));
      Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_MISSING_ARGS,
          ingestZipDriver.run(new String[] { CompaniesBaseTestCase.getPathLocal("") }));
      Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_MISSING_ARGS, ingestZipDriver.run(new String[] { inputDir }));
      Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
          ingestZipDriver.run(new String[] { CompaniesBaseTestCase.getPathLocal(inputFile), outputDir }));
      Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
          ingestZipDriver.run(new String[] { CompaniesBaseTestCase.getPathLocal(inputNonExistantDir), outputDir }));
      Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
          ingestZipDriver.run(new String[] { inputDir, outputFile }));

      if (!UserGroupInformation.getCurrentUser().getUserName().equals("root")) {

        getFileSystem()
            .setPermission(new Path(inputDir), new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.NONE));

        Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
            ingestZipDriver.run(new String[] { inputDir, outputDir }));

        getFileSystem()
            .setPermission(new Path(inputDir), new FsPermission(FsAction.READ, FsAction.NONE, FsAction.NONE));

        Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
            ingestZipDriver.run(new String[] { inputDir, outputDir }));

        getFileSystem().setPermission(new Path(inputDir),
            new FsPermission(FsAction.WRITE, FsAction.NONE, FsAction.NONE));

        Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
            ingestZipDriver.run(new String[] { inputDir, outputDir }));

        getFileSystem().setPermission(new Path(inputDir),
            new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE));

        Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
            ingestZipDriver.run(new String[] { inputDir, outputDir }));

        getFileSystem().setPermission(new Path(inputDir), new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
        getFileSystem().setPermission(new Path(outputDir),
            new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.NONE));

        Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
            ingestZipDriver.run(new String[] { inputDir, outputDir }));

        getFileSystem().setPermission(new Path(outputDir),
            new FsPermission(FsAction.READ, FsAction.NONE, FsAction.NONE));

        Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
            ingestZipDriver.run(new String[] { inputDir, outputDir }));

        getFileSystem().setPermission(new Path(outputDir),
            new FsPermission(FsAction.WRITE, FsAction.NONE, FsAction.NONE));

        Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
            ingestZipDriver.run(new String[] { inputDir, outputDir }));

        getFileSystem().setPermission(new Path(outputDir),
            new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE));

        Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
            ingestZipDriver.run(new String[] { inputDir, outputDir }));

        getFileSystem().setPermission(new Path(outputDir),
            new FsPermission(FsAction.EXECUTE, FsAction.NONE, FsAction.NONE));

        Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
            ingestZipDriver.run(new String[] { inputDir, outputDir }));

        getFileSystem().setPermission(new Path(outputDir),
            new FsPermission(FsAction.READ_EXECUTE, FsAction.NONE, FsAction.NONE));

        Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
            ingestZipDriver.run(new String[] { inputDir, outputDir }));

        getFileSystem().setPermission(new Path(outputDir),
            new FsPermission(FsAction.WRITE_EXECUTE, FsAction.NONE, FsAction.NONE));

        Assert.assertEquals(CompaniesDriver.RETURN_FAILURE_INVALID_ARGS,
            ingestZipDriver.run(new String[] { inputDir, outputDir }));

      }
    } finally {
      getFileSystem().setPermission(new Path(inputDir), new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
      getFileSystem().setPermission(new Path(outputDir), new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
    }
  }

  @Test
  public void testSingleThread() throws Exception {

    Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS,
        ingestZipDriver.run(new String[] { PATH_LOCAL_INPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_ZIP }));
    Assert.assertEquals(1, getFileSystem().listStatus(new Path(PATH_HDFS_OUTPUT_DIR_ZIP)).length);
    for (FileStatus yearFileStatus : getFileSystem().listStatus(new Path(PATH_HDFS_OUTPUT_DIR_ZIP))) {
      Assert.assertEquals(2, getFileSystem().listStatus(yearFileStatus.getPath()).length);
      for (FileStatus monthFileStatus : getFileSystem().listStatus(yearFileStatus.getPath())) {
        Assert.assertEquals(5, getFileSystem().listStatus(monthFileStatus.getPath()).length);
        Assert.assertTrue(getFileSystem().exists(
            new Path(monthFileStatus.getPath(), CompaniesDriver.CONF_MR_FILECOMMITTER_SUCCEEDED_FILE_NAME)));
      }
    }
  }

  @Test
  public void testMultiThread() throws Exception {
    getFileSystem().getConf().set(IngestZipDriver.CONF_THREAD_NUMBER, "3");
    try {
      testSingleThread();
    } finally {
      getFileSystem().getConf().set(IngestZipDriver.CONF_THREAD_NUMBER, "1");
    }
  }

}

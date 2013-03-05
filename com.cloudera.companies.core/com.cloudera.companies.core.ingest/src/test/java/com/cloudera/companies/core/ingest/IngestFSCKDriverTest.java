package com.cloudera.companies.core.ingest;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.companies.core.common.CompaniesDriver;
import com.cloudera.companies.core.ingest.seq.IngestSeqDriver;
import com.cloudera.companies.core.ingest.zip.IngestZipDriver;
import com.cloudera.companies.core.test.CompaniesEmbeddedCoreTestCase;

public class IngestFSCKDriverTest extends CompaniesEmbeddedCoreTestCase {

  private IngestFSCKDriver ingestFSCKDriver;
  private IngestZipDriver ingestZipDriver;
  private IngestSeqDriver ingestSeqDriver;

  public IngestFSCKDriverTest() throws IOException {
    super();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    ingestFSCKDriver = new IngestFSCKDriver(getFileSystem().getConf());
    ingestZipDriver = new IngestZipDriver(getFileSystem().getConf());
    ingestSeqDriver = new IngestSeqDriver(getFileSystem().getConf());
  }

  @Test
  public void test() throws Exception {
    Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS,
        ingestZipDriver.run(new String[] { PATH_LOCAL_INPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_ZIP }));
    Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS,
        ingestSeqDriver.run(new String[] { PATH_HDFS_OUTPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_SEQ }));
    Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS,
        ingestFSCKDriver.run(new String[] { PATH_HDFS_OUTPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_SEQ }));
    Assert.assertEquals(true, ingestFSCKDriver.testIntegretity(2, 8, 3).isEmpty());
  }

  @Test
  public void testDirty() throws Exception {
    Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS,
        ingestZipDriver.run(new String[] { PATH_LOCAL_INPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_ZIP }));
    Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS,
        ingestSeqDriver.run(new String[] { PATH_HDFS_OUTPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_SEQ }));
    getFileSystem().createNewFile(new Path(PATH_HDFS_OUTPUT_DIR_ZIP + "/test1.txt"));
    getFileSystem().createNewFile(
        new Path(PATH_HDFS_OUTPUT_DIR_ZIP + "/2012/05/BasicCompanyData-2012-05-01-part5_4.zip"));
    getFileSystem().createNewFile(new Path(PATH_HDFS_OUTPUT_DIR_SEQ + "/2012/test3.txt"));
    Assert.assertEquals(CompaniesDriver.RETURN_WARNING_DIRTY_INGEST,
        ingestFSCKDriver.run(new String[] { PATH_HDFS_OUTPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_SEQ }));
    Assert.assertEquals(false, ingestFSCKDriver.testIntegretity(2, 8, 3).isEmpty());
  }

  @Test
  public void testClean() throws Exception {
    getFileSystem().getConf().set(IngestFSCKDriver.CONF_FSCK_CLEAN, Boolean.TRUE.toString());
    Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS,
        ingestZipDriver.run(new String[] { PATH_LOCAL_INPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_ZIP }));
    Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS,
        ingestSeqDriver.run(new String[] { PATH_HDFS_OUTPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_SEQ }));
    getFileSystem().createNewFile(new Path(PATH_HDFS_OUTPUT_DIR_ZIP + "/test1.txt"));
    getFileSystem().createNewFile(new Path(PATH_HDFS_OUTPUT_DIR_SEQ + "/2012/test3.txt"));
    Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS,
        ingestFSCKDriver.run(new String[] { PATH_HDFS_OUTPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_SEQ }));
    Assert.assertEquals(false, ingestFSCKDriver.testIntegretity(2, 8, 3).isEmpty());
    Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS,
        ingestFSCKDriver.run(new String[] { PATH_HDFS_OUTPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_SEQ }));
    Assert.assertEquals(true, ingestFSCKDriver.testIntegretity(2, 8, 3).isEmpty());
  }

  @Test
  public void testCleanCorupt() throws Exception {
    getFileSystem().getConf().set(IngestFSCKDriver.CONF_FSCK_CLEAN, Boolean.TRUE.toString());
    Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS,
        ingestZipDriver.run(new String[] { PATH_LOCAL_INPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_ZIP }));
    Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS,
        ingestSeqDriver.run(new String[] { PATH_HDFS_OUTPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_SEQ }));
    getFileSystem().createNewFile(new Path(PATH_HDFS_OUTPUT_DIR_ZIP + "/test1.txt"));
    getFileSystem().createNewFile(
        new Path(PATH_HDFS_OUTPUT_DIR_ZIP + "/2012/05/BasicCompanyData-2012-05-01-part5_4.zip"));
    getFileSystem().createNewFile(new Path(PATH_HDFS_OUTPUT_DIR_SEQ + "/2012/test3.txt"));
    Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS,
        ingestFSCKDriver.run(new String[] { PATH_HDFS_OUTPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_SEQ }));
    Assert.assertEquals(false, ingestFSCKDriver.testIntegretity(2, 8, 3).isEmpty());
    Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS,
        ingestFSCKDriver.run(new String[] { PATH_HDFS_OUTPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_SEQ }));
    Assert.assertEquals(false, ingestFSCKDriver.testIntegretity(2, 8, 3).isEmpty());
  }

  @Test
  public void testMissingSeq() throws Exception {
    Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS,
        ingestZipDriver.run(new String[] { PATH_LOCAL_INPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_ZIP }));
    getFileSystem().mkdirs(new Path(PATH_HDFS_OUTPUT_DIR_SEQ));
    Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS,
        ingestFSCKDriver.run(new String[] { PATH_HDFS_OUTPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_SEQ }));
    Assert.assertEquals(false, ingestFSCKDriver.testIntegretity(2, 8, 3).isEmpty());
  }

  @Test
  public void testMissingAll() throws Exception {
    getFileSystem().mkdirs(new Path(PATH_HDFS_OUTPUT_DIR_ZIP));
    getFileSystem().mkdirs(new Path(PATH_HDFS_OUTPUT_DIR_SEQ));
    Assert.assertEquals(CompaniesDriver.RETURN_SUCCESS,
        ingestFSCKDriver.run(new String[] { PATH_HDFS_OUTPUT_DIR_ZIP, PATH_HDFS_OUTPUT_DIR_SEQ }));
    Assert.assertEquals(false, ingestFSCKDriver.testIntegretity(2, 8, 3).isEmpty());
  }

}

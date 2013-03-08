package com.cloudera.companies.core.common;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.companies.core.test.CompaniesBaseTestCase;

public class CompaniesFileTest extends CompaniesBaseTestCase {

  @Test
  public void testParsePath() throws IOException {

    boolean thrown = false;

    thrown = false;
    try {
      Assert.assertNull(CompaniesFileMetaData.parsePathZip(null, null));
    } catch (IllegalArgumentException e) {
      thrown = true;
    }
    Assert.assertTrue(thrown);

    thrown = false;
    try {
      Assert.assertNull(CompaniesFileMetaData.parsePathZip(new File(".").getName(), new File(".").getParent()));
    } catch (IllegalArgumentException e) {
      thrown = true;
    }
    Assert.assertTrue(thrown);

    thrown = false;
    try {
      Assert.assertNull(CompaniesFileMetaData.parsePathZip(new File("./target/BasicCompanyData-201-05-01-part1_4.zip")
          .getName(), new File("./target/BasicCompanyData-201-05-01-part1_4.zip").getParent()));
    } catch (IOException e) {
      thrown = true;
    }
    Assert.assertTrue(thrown);

    thrown = false;
    try {
      Assert.assertNull(CompaniesFileMetaData.parsePathZip(new File("./target/BasicCompanyData-2012-O5-01-part1_4.zip")
          .getName(), new File("./target/BasicCompanyData-2012-O5-01-part1_4.zip").getParent()));
    } catch (IOException e) {
      thrown = true;
    }
    Assert.assertTrue(thrown);

    thrown = false;
    try {
      Assert.assertNull(CompaniesFileMetaData.parsePathZip(new File("./target/BasicCompanyData-2012-05-1-part1_4.zip")
          .getName(), new File("./target/BasicCompanyData-2012-05-1-part1_4.zip").getParent()));
    } catch (IOException e) {
      thrown = true;
    }
    Assert.assertTrue(thrown);

    File fileZip = new File("./target/BasicCompanyData-2012-05-01-part1_4.zip");
    Assert.assertEquals("BasicCompanyData-2012-05-01-part1_4.zip",
        CompaniesFileMetaData.parsePathZip(fileZip.getName(), fileZip.getParent()).getName());
    Assert.assertEquals("./target", CompaniesFileMetaData.parsePathZip(fileZip.getName(), fileZip.getParent())
        .getDirectory());
    Assert.assertEquals("snapshot_year=2012/snapshot_month=05", CompaniesFileMetaData.parsePathZip(fileZip.getName(), fileZip.getParent())
        .getGroup());
    Assert.assertEquals(new Date(1335830400000L),
        CompaniesFileMetaData.parsePathZip(fileZip.getName(), fileZip.getParent()).getSnapshotDate());
    Assert.assertEquals(1, CompaniesFileMetaData.parsePathZip(fileZip.getName(), fileZip.getParent()).getPart());
    Assert.assertEquals(4, CompaniesFileMetaData.parsePathZip(fileZip.getName(), fileZip.getParent()).getPartTotal());

    File fileCSV = new File("./target/BasicCompanyData-2012-05-01-part1_4.csv");
    Assert.assertEquals("BasicCompanyData-2012-05-01-part1_4.csv",
        CompaniesFileMetaData.parsePathCSV(fileCSV.getName(), fileCSV.getParent()).getName());
    Assert.assertEquals("./target", CompaniesFileMetaData.parsePathCSV(fileCSV.getName(), fileCSV.getParent())
        .getDirectory());
    Assert.assertEquals("snapshot_year=2012/snapshot_month=05", CompaniesFileMetaData.parsePathCSV(fileCSV.getName(), fileCSV.getParent())
        .getGroup());
    Assert.assertEquals(new Date(1335830400000L),
        CompaniesFileMetaData.parsePathCSV(fileCSV.getName(), fileCSV.getParent()).getSnapshotDate());
    Assert.assertEquals(1, CompaniesFileMetaData.parsePathCSV(fileCSV.getName(), fileCSV.getParent()).getPart());
    Assert.assertEquals(4, CompaniesFileMetaData.parsePathCSV(fileCSV.getName(), fileCSV.getParent()).getPartTotal());

    File fileReduce = new File("./target/MAY-2012-r-00000");
    Assert.assertEquals("MAY-2012-r-00000",
        CompaniesFileMetaData.parsePathSeq(fileReduce.getName(), fileReduce.getParent()).getName());
    Assert.assertEquals("./target", CompaniesFileMetaData.parsePathSeq(fileReduce.getName(), fileReduce.getParent())
        .getDirectory());
    Assert.assertEquals("snapshot_year=2012/snapshot_month=05", CompaniesFileMetaData.parsePathSeq(fileReduce.getName(), fileReduce.getParent())
        .getGroup());
    Assert.assertEquals(new Date(1335830400000L),
        CompaniesFileMetaData.parsePathSeq(fileReduce.getName(), fileReduce.getParent()).getSnapshotDate());
    Assert.assertEquals(1, CompaniesFileMetaData.parsePathSeq(fileReduce.getName(), fileReduce.getParent()).getPart());
    Assert.assertEquals(1, CompaniesFileMetaData.parsePathSeq(fileReduce.getName(), fileReduce.getParent())
        .getPartTotal());

  }

  @Test
  public void testParseRecord() throws IOException {

    Assert.assertArrayEquals(new String[] { "" }, CompaniesFileMetaData.parseRecord(""));
    Assert.assertArrayEquals(new String[] { "" }, CompaniesFileMetaData.parseRecord("A"));
    Assert.assertArrayEquals(new String[] { "" }, CompaniesFileMetaData.parseRecord("AA"));
    Assert.assertArrayEquals(new String[] { "" }, CompaniesFileMetaData.parseRecord("AA BB"));
    Assert.assertArrayEquals(new String[] { "" }, CompaniesFileMetaData.parseRecord(" AA BB "));
    Assert.assertArrayEquals(new String[] { "" },
        CompaniesFileMetaData.parseRecord("" + CompaniesFileMetaData.FILE_ESCAPE_CHAR));

    Assert.assertArrayEquals(new String[] { "A" }, CompaniesFileMetaData.parseRecord("\"A\"A"));
    Assert.assertArrayEquals(new String[] { "A" },
        CompaniesFileMetaData.parseRecord("\"A\"" + CompaniesFileMetaData.FILE_ESCAPE_CHAR));
    Assert.assertArrayEquals(new String[] { "A" + CompaniesFileMetaData.FILE_ESCAPE_CHAR },
        CompaniesFileMetaData.parseRecord("\"A" + CompaniesFileMetaData.FILE_ESCAPE_CHAR + "\""));
    Assert.assertArrayEquals(new String[] { "AA" }, CompaniesFileMetaData.parseRecord("\"AA\""));
    Assert.assertArrayEquals(new String[] { "AA BB" }, CompaniesFileMetaData.parseRecord("\"AA BB\""));
    Assert.assertArrayEquals(new String[] { "AA BB" }, CompaniesFileMetaData.parseRecord(" \"AA BB\" "));
    Assert.assertArrayEquals(new String[] { " AA BB " }, CompaniesFileMetaData.parseRecord("\" AA BB \""));
    Assert.assertArrayEquals(new String[] { "AA BB\\" }, CompaniesFileMetaData.parseRecord("\"AA BB\\\""));
    Assert.assertArrayEquals(new String[] { "AA\" BB" }, CompaniesFileMetaData.parseRecord("\"AA\"\" BB\""));
    Assert.assertArrayEquals(new String[] { "\"AA BB\"" }, CompaniesFileMetaData.parseRecord("\"\"\"AA BB\"\"\""));
    Assert.assertArrayEquals(new String[] { "\"AA BB\"" }, CompaniesFileMetaData.parseRecord(" \"\"\"AA BB\"\"\" "));
    Assert.assertArrayEquals(new String[] { "\"AA,BB\"" }, CompaniesFileMetaData.parseRecord(" \"\"\"AA,BB\"\"\" "));

    Assert.assertArrayEquals(new String[] { "A", "A" }, CompaniesFileMetaData.parseRecord("\"A\",\"A\""));
    Assert.assertArrayEquals(new String[] { "AA", "AA" }, CompaniesFileMetaData.parseRecord("\"AA\",\"AA\""));
    Assert.assertArrayEquals(new String[] { "AA BB", "AA BB" },
        CompaniesFileMetaData.parseRecord("\"AA BB\",\"AA BB\""));
    Assert.assertArrayEquals(new String[] { "AA BB", "AA BB" },
        CompaniesFileMetaData.parseRecord(" \"AA BB\" , \"AA BB\" "));
    Assert.assertArrayEquals(new String[] { " AA BB ", " AA BB " },
        CompaniesFileMetaData.parseRecord("\" AA BB \",\" AA BB \""));
    Assert.assertArrayEquals(new String[] { "AA BB\\", "AA BB\\" },
        CompaniesFileMetaData.parseRecord("\"AA BB\\\",\"AA BB\\\""));
    Assert.assertArrayEquals(new String[] { "AA\" BB", "AA\" BB" },
        CompaniesFileMetaData.parseRecord("\"AA\"\" BB\",\"AA\"\" BB\""));
    Assert.assertArrayEquals(new String[] { "\"AA BB\"", "\"AA BB\"" },
        CompaniesFileMetaData.parseRecord("\"\"\"AA BB\"\"\",\"\"\"AA BB\"\"\""));
    Assert.assertArrayEquals(new String[] { "\"AA BB\"", "\"AA BB\"" },
        CompaniesFileMetaData.parseRecord(" \"\"\"AA BB\"\"\" , \"\"\"AA BB\"\"\" "));
    Assert.assertArrayEquals(new String[] { "\"AA,BB\"", "\"AA,BB\"" },
        CompaniesFileMetaData.parseRecord(" \"\"\"AA,BB\"\"\" , \"\"\"AA,BB\"\"\" "));

  }

}

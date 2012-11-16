package com.cloudera.companies.core.common;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

public class CompaniesFileTest {

	@Test
	public void testParseFile() throws IOException {

		boolean thrown = false;

		thrown = false;
		try {
			Assert.assertNull(CompaniesFileMetaData.parseFile(null, null));
		} catch (IllegalArgumentException e) {
			thrown = true;
		}
		Assert.assertTrue(thrown);

		thrown = false;
		try {
			Assert.assertNull(CompaniesFileMetaData.parseFile(new File(".").getName(), new File(".").getParent()));
		} catch (IllegalArgumentException e) {
			thrown = true;
		}
		Assert.assertTrue(thrown);

		thrown = false;
		try {
			Assert.assertNull(CompaniesFileMetaData.parseFile(new File(
					"./target/BasicCompanyData-201-05-01-part1_4.zip").getName(), new File(
					"./target/BasicCompanyData-201-05-01-part1_4.zip").getParent()));
		} catch (IOException e) {
			thrown = true;
		}
		Assert.assertTrue(thrown);

		thrown = false;
		try {
			Assert.assertNull(CompaniesFileMetaData.parseFile(new File(
					"./target/BasicCompanyData-2012-O5-01-part1_4.zip").getName(), new File(
					"./target/BasicCompanyData-2012-O5-01-part1_4.zip").getParent()));
		} catch (IOException e) {
			thrown = true;
		}
		Assert.assertTrue(thrown);

		thrown = false;
		try {
			Assert.assertNull(CompaniesFileMetaData.parseFile(new File(
					"./target/BasicCompanyData-2012-05-1-part1_4.zip").getName(), new File(
					"./target/BasicCompanyData-2012-05-1-part1_4.zip").getParent()));
		} catch (IOException e) {
			thrown = true;
		}
		Assert.assertTrue(thrown);

		int part = 1;
		File testDataDir = new File("./target/test-data/data/basiccompany/sample/zip");
		for (File testDataFile : testDataDir.listFiles()) {
			CompaniesFileMetaData companiesFile = CompaniesFileMetaData.parseFile(testDataFile.getName(),
					testDataFile.getParent());
			Assert.assertNotNull(companiesFile);
			Assert.assertEquals(testDataFile.getName(), companiesFile.getName());
			Assert.assertEquals(testDataFile.getParent(), companiesFile.getDirectory());
			Assert.assertNotNull(companiesFile.getGroup());
			Assert.assertNotNull(companiesFile.getSnapshotDate());
			Assert.assertTrue(companiesFile.getPart() < 5 && companiesFile.getPart() > 0);
			Assert.assertEquals(4, companiesFile.getPartTotal());
		}

	}

	@Test
	public void testParseRecord() throws IOException {

		boolean thrown = false;
		try {
			Assert.assertNull(CompaniesFileMetaData.parseRecord(null));
		} catch (IllegalArgumentException e) {
			thrown = true;
		}
		Assert.assertTrue(thrown);

		Assert.assertArrayEquals(new String[] { "" }, CompaniesFileMetaData.parseRecord(""));
		Assert.assertArrayEquals(new String[] { "" }, CompaniesFileMetaData.parseRecord("A"));
		Assert.assertArrayEquals(new String[] { "" }, CompaniesFileMetaData.parseRecord("AA"));
		Assert.assertArrayEquals(new String[] { "" }, CompaniesFileMetaData.parseRecord("AA BB"));
		Assert.assertArrayEquals(new String[] { "" }, CompaniesFileMetaData.parseRecord(" AA BB "));

		Assert.assertArrayEquals(new String[] { "A" }, CompaniesFileMetaData.parseRecord("\"A\""));
		Assert.assertArrayEquals(new String[] { "AA" }, CompaniesFileMetaData.parseRecord("\"AA\""));
		Assert.assertArrayEquals(new String[] { "AA BB" }, CompaniesFileMetaData.parseRecord("\"AA BB\""));
		Assert.assertArrayEquals(new String[] { "AA BB" }, CompaniesFileMetaData.parseRecord(" \"AA BB\" "));
		Assert.assertArrayEquals(new String[] { " AA BB " }, CompaniesFileMetaData.parseRecord("\" AA BB \""));
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

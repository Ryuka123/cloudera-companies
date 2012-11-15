package com.cloudera.companies.core.common;

import java.io.File;
import java.io.IOException;
import java.sql.Date;

import org.junit.Assert;
import org.junit.Test;

public class CompaniesFileTest {

	@Test
	public void testParseFileName() throws IOException {

		boolean thrown = false;

		thrown = false;
		try {
			Assert.assertNull(CompaniesFile.parseFileName(null));
		} catch (IllegalArgumentException e) {
			thrown = true;
		}
		Assert.assertTrue(thrown);

		thrown = false;
		try {
			Assert.assertNull(CompaniesFile.parseFileName(new File(".")));
		} catch (IllegalArgumentException e) {
			thrown = true;
		}
		Assert.assertTrue(thrown);

		thrown = false;
		try {
			Assert.assertNull(CompaniesFile.parseFileName(new File("./target/BasicCompanyData-201-05-01-part1_4.zip")));
		} catch (IllegalArgumentException e) {
			thrown = true;
		}
		Assert.assertTrue(thrown);

		thrown = false;
		try {
			Assert.assertNull(CompaniesFile.parseFileName(new File("./target/BasicCompanyData-2012-O5-01-part1_4.zip")));
		} catch (IllegalArgumentException e) {
			thrown = true;
		}
		Assert.assertTrue(thrown);

		thrown = false;
		try {
			Assert.assertNull(CompaniesFile.parseFileName(new File("./target/BasicCompanyData-2012-05-1-part1_4.zip")));
		} catch (IllegalArgumentException e) {
			thrown = true;
		}
		Assert.assertTrue(thrown);

		thrown = false;
		try {
			Assert.assertNull(CompaniesFile.parseFileName(new File("./target/BasicCompanyData-2012-05-01-part1_4.zip")));
		} catch (IllegalArgumentException e) {
			thrown = true;
		}
		Assert.assertTrue(thrown);

		int part = 1;
		File testDataDir = new File("./target/test-input/data/basiccompany/sample/zip");
		for (File testDataFile : testDataDir.listFiles()) {
			CompaniesFile companiesFile = CompaniesFile.parseFileName(testDataFile);
			Assert.assertNotNull(companiesFile);
			Assert.assertEquals(testDataFile.getCanonicalPath(), companiesFile.getFile().getCanonicalPath());
			Assert.assertEquals(new Date(1335826800000L), companiesFile.getSnapshotDate());
			Assert.assertEquals(part++, companiesFile.getPart());
			Assert.assertEquals(4, companiesFile.getPartTotal());
		}

	}

	@Test
	public void testParseRecord() throws IOException {

		boolean thrown = false;
		try {
			Assert.assertNull(CompaniesFile.parseRecord(null));
		} catch (IllegalArgumentException e) {
			thrown = true;
		}
		Assert.assertTrue(thrown);

		Assert.assertArrayEquals(new String[] { "" }, CompaniesFile.parseRecord(""));
		Assert.assertArrayEquals(new String[] { "" }, CompaniesFile.parseRecord("A"));
		Assert.assertArrayEquals(new String[] { "" }, CompaniesFile.parseRecord("AA"));
		Assert.assertArrayEquals(new String[] { "" }, CompaniesFile.parseRecord("AA BB"));
		Assert.assertArrayEquals(new String[] { "" }, CompaniesFile.parseRecord(" AA BB "));

		Assert.assertArrayEquals(new String[] { "A" }, CompaniesFile.parseRecord("\"A\""));
		Assert.assertArrayEquals(new String[] { "AA" }, CompaniesFile.parseRecord("\"AA\""));
		Assert.assertArrayEquals(new String[] { "AA BB" }, CompaniesFile.parseRecord("\"AA BB\""));
		Assert.assertArrayEquals(new String[] { "AA BB" }, CompaniesFile.parseRecord(" \"AA BB\" "));
		Assert.assertArrayEquals(new String[] { " AA BB " }, CompaniesFile.parseRecord("\" AA BB \""));
		Assert.assertArrayEquals(new String[] { "AA\" BB" }, CompaniesFile.parseRecord("\"AA\"\" BB\""));
		Assert.assertArrayEquals(new String[] { "\"AA BB\"" }, CompaniesFile.parseRecord("\"\"\"AA BB\"\"\""));
		Assert.assertArrayEquals(new String[] { "\"AA BB\"" }, CompaniesFile.parseRecord(" \"\"\"AA BB\"\"\" "));
		Assert.assertArrayEquals(new String[] { "\"AA,BB\"" }, CompaniesFile.parseRecord(" \"\"\"AA,BB\"\"\" "));

		Assert.assertArrayEquals(new String[] { "A", "A" }, CompaniesFile.parseRecord("\"A\",\"A\""));
		Assert.assertArrayEquals(new String[] { "AA", "AA" }, CompaniesFile.parseRecord("\"AA\",\"AA\""));
		Assert.assertArrayEquals(new String[] { "AA BB", "AA BB" }, CompaniesFile.parseRecord("\"AA BB\",\"AA BB\""));
		Assert.assertArrayEquals(new String[] { "AA BB", "AA BB" },
				CompaniesFile.parseRecord(" \"AA BB\" , \"AA BB\" "));
		Assert.assertArrayEquals(new String[] { " AA BB ", " AA BB " },
				CompaniesFile.parseRecord("\" AA BB \",\" AA BB \""));
		Assert.assertArrayEquals(new String[] { "AA\" BB", "AA\" BB" },
				CompaniesFile.parseRecord("\"AA\"\" BB\",\"AA\"\" BB\""));
		Assert.assertArrayEquals(new String[] { "\"AA BB\"", "\"AA BB\"" },
				CompaniesFile.parseRecord("\"\"\"AA BB\"\"\",\"\"\"AA BB\"\"\""));
		Assert.assertArrayEquals(new String[] { "\"AA BB\"", "\"AA BB\"" },
				CompaniesFile.parseRecord(" \"\"\"AA BB\"\"\" , \"\"\"AA BB\"\"\" "));
		Assert.assertArrayEquals(new String[] { "\"AA,BB\"", "\"AA,BB\"" },
				CompaniesFile.parseRecord(" \"\"\"AA,BB\"\"\" , \"\"\"AA,BB\"\"\" "));

	}
}

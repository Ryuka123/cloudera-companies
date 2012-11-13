package com.cloudera.companies.core.common;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

public class CompaniesRecordParserTest {

	@Test
	public void testParser() throws IOException {

		Assert.assertNull(CompaniesRecordParser.parse(null));

		Assert.assertArrayEquals(new String[] { "" }, CompaniesRecordParser.parse(""));
		Assert.assertArrayEquals(new String[] { "" }, CompaniesRecordParser.parse("A"));
		Assert.assertArrayEquals(new String[] { "" }, CompaniesRecordParser.parse("AA"));
		Assert.assertArrayEquals(new String[] { "" }, CompaniesRecordParser.parse("AA BB"));
		Assert.assertArrayEquals(new String[] { "" }, CompaniesRecordParser.parse(" AA BB "));

		Assert.assertArrayEquals(new String[] { "A" }, CompaniesRecordParser.parse("\"A\""));
		Assert.assertArrayEquals(new String[] { "AA" }, CompaniesRecordParser.parse("\"AA\""));
		Assert.assertArrayEquals(new String[] { "AA BB" }, CompaniesRecordParser.parse("\"AA BB\""));
		Assert.assertArrayEquals(new String[] { "AA BB" }, CompaniesRecordParser.parse(" \"AA BB\" "));
		Assert.assertArrayEquals(new String[] { " AA BB " }, CompaniesRecordParser.parse("\" AA BB \""));
		Assert.assertArrayEquals(new String[] { "AA\" BB" }, CompaniesRecordParser.parse("\"AA\"\" BB\""));
		Assert.assertArrayEquals(new String[] { "\"AA BB\"" }, CompaniesRecordParser.parse("\"\"\"AA BB\"\"\""));
		Assert.assertArrayEquals(new String[] { "\"AA BB\"" }, CompaniesRecordParser.parse(" \"\"\"AA BB\"\"\" "));
		Assert.assertArrayEquals(new String[] { "\"AA,BB\"" }, CompaniesRecordParser.parse(" \"\"\"AA,BB\"\"\" "));

		Assert.assertArrayEquals(new String[] { "A", "A" }, CompaniesRecordParser.parse("\"A\",\"A\""));
		Assert.assertArrayEquals(new String[] { "AA", "AA" }, CompaniesRecordParser.parse("\"AA\",\"AA\""));
		Assert.assertArrayEquals(new String[] { "AA BB", "AA BB" }, CompaniesRecordParser.parse("\"AA BB\",\"AA BB\""));
		Assert.assertArrayEquals(new String[] { "AA BB", "AA BB" }, CompaniesRecordParser.parse(" \"AA BB\" , \"AA BB\" "));
		Assert.assertArrayEquals(new String[] { " AA BB ", " AA BB " }, CompaniesRecordParser.parse("\" AA BB \",\" AA BB \""));
		Assert.assertArrayEquals(new String[] { "AA\" BB", "AA\" BB" },
				CompaniesRecordParser.parse("\"AA\"\" BB\",\"AA\"\" BB\""));
		Assert.assertArrayEquals(new String[] { "\"AA BB\"", "\"AA BB\"" },
				CompaniesRecordParser.parse("\"\"\"AA BB\"\"\",\"\"\"AA BB\"\"\""));
		Assert.assertArrayEquals(new String[] { "\"AA BB\"", "\"AA BB\"" },
				CompaniesRecordParser.parse(" \"\"\"AA BB\"\"\" , \"\"\"AA BB\"\"\" "));
		Assert.assertArrayEquals(new String[] { "\"AA,BB\"", "\"AA,BB\"" },
				CompaniesRecordParser.parse(" \"\"\"AA,BB\"\"\" , \"\"\"AA,BB\"\"\" "));

	}
}

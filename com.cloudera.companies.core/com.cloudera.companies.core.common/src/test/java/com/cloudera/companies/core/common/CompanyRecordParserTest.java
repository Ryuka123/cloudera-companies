package com.cloudera.companies.core.common;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

public class CompanyRecordParserTest {

	@Test
	public void testParser() throws IOException {

		Assert.assertNull(CompanyRecordParser.parse(null));

		Assert.assertArrayEquals(new String[] { "" }, CompanyRecordParser.parse(""));
		Assert.assertArrayEquals(new String[] { "" }, CompanyRecordParser.parse("A"));
		Assert.assertArrayEquals(new String[] { "" }, CompanyRecordParser.parse("AA"));
		Assert.assertArrayEquals(new String[] { "" }, CompanyRecordParser.parse("AA BB"));
		Assert.assertArrayEquals(new String[] { "" }, CompanyRecordParser.parse(" AA BB "));

		Assert.assertArrayEquals(new String[] { "A" }, CompanyRecordParser.parse("\"A\""));
		Assert.assertArrayEquals(new String[] { "AA" }, CompanyRecordParser.parse("\"AA\""));
		Assert.assertArrayEquals(new String[] { "AA BB" }, CompanyRecordParser.parse("\"AA BB\""));
		Assert.assertArrayEquals(new String[] { "AA BB" }, CompanyRecordParser.parse(" \"AA BB\" "));
		Assert.assertArrayEquals(new String[] { " AA BB " }, CompanyRecordParser.parse("\" AA BB \""));
		Assert.assertArrayEquals(new String[] { "AA\" BB" }, CompanyRecordParser.parse("\"AA\"\" BB\""));
		Assert.assertArrayEquals(new String[] { "\"AA BB\"" }, CompanyRecordParser.parse("\"\"\"AA BB\"\"\""));
		Assert.assertArrayEquals(new String[] { "\"AA BB\"" }, CompanyRecordParser.parse(" \"\"\"AA BB\"\"\" "));
		Assert.assertArrayEquals(new String[] { "\"AA,BB\"" }, CompanyRecordParser.parse(" \"\"\"AA,BB\"\"\" "));

		Assert.assertArrayEquals(new String[] { "A", "A" }, CompanyRecordParser.parse("\"A\",\"A\""));
		Assert.assertArrayEquals(new String[] { "AA", "AA" }, CompanyRecordParser.parse("\"AA\",\"AA\""));
		Assert.assertArrayEquals(new String[] { "AA BB", "AA BB" }, CompanyRecordParser.parse("\"AA BB\",\"AA BB\""));
		Assert.assertArrayEquals(new String[] { "AA BB", "AA BB" }, CompanyRecordParser.parse(" \"AA BB\" , \"AA BB\" "));
		Assert.assertArrayEquals(new String[] { " AA BB ", " AA BB " }, CompanyRecordParser.parse("\" AA BB \",\" AA BB \""));
		Assert.assertArrayEquals(new String[] { "AA\" BB", "AA\" BB" },
				CompanyRecordParser.parse("\"AA\"\" BB\",\"AA\"\" BB\""));
		Assert.assertArrayEquals(new String[] { "\"AA BB\"", "\"AA BB\"" },
				CompanyRecordParser.parse("\"\"\"AA BB\"\"\",\"\"\"AA BB\"\"\""));
		Assert.assertArrayEquals(new String[] { "\"AA BB\"", "\"AA BB\"" },
				CompanyRecordParser.parse(" \"\"\"AA BB\"\"\" , \"\"\"AA BB\"\"\" "));
		Assert.assertArrayEquals(new String[] { "\"AA,BB\"", "\"AA,BB\"" },
				CompanyRecordParser.parse(" \"\"\"AA,BB\"\"\" , \"\"\"AA,BB\"\"\" "));

	}
}

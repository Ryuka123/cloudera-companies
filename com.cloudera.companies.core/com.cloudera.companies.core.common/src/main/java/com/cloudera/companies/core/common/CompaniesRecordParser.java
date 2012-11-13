package com.cloudera.companies.core.common;

import java.io.IOException;

import au.com.bytecode.opencsv.CSVParser;

public class CompaniesRecordParser {

	private static final CSVParser CSV_PARSER = new CSVParser(',', '"', '\\', true, true);

	public static String[] parse(String record) throws IOException {
		return CSV_PARSER.parseLine(record);
	}
}

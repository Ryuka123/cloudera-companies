package com.cloudera.companies.core.common;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import au.com.bytecode.opencsv.CSVParser;

public class CompaniesFileMetaData {

	public static final int FILE_FIELDS_NUMBER = 53;

	private static final String FILE_NAME_REGEX_BASE = "BasicCompanyData-(20[0-9][0-9]-[0-1][0-9]-[0-3][0-9])-part([1-9])_([1-9])";
	private static final Pattern FILE_NAME_PATTERN_ZIP = Pattern.compile(FILE_NAME_REGEX_BASE + ".zip");
	private static final Pattern FILE_NAME_PATTERN_CSV = Pattern.compile(FILE_NAME_REGEX_BASE + ".csv");

	private static final String DATE_FORMAT_FILE_NAME = "yyyy-MM-dd";
	private static final String DATE_FORMAT_GROUP = "yyyy/MM";
	private static final String DATE_FORMAT_GROUP_FILE = "yyyy/MM/MMM-yyyy";

	private static final CSVParser FILE_RECORD_PARSER = new CSVParser(',', '"', '\\', true, true);

	private String name;
	private String directory;
	private String group;
	private int part;
	private int partTotal;
	private Date snapshotDate;

	public static CompaniesFileMetaData parsePathZip(String name, String directory) throws IOException {
		return parsePath(FILE_NAME_PATTERN_ZIP, name, directory);
	}

	public static CompaniesFileMetaData parsePathCSV(String name, String directory) throws IOException {
		return parsePath(FILE_NAME_PATTERN_CSV, name, directory);
	}

	private static CompaniesFileMetaData parsePath(Pattern pattern, String name, String directory) throws IOException {

		if (name == null) {
			throw new IllegalArgumentException("null name");
		}
		if (directory == null) {
			throw new IllegalArgumentException("null directory");
		}
		Matcher fileNameMatcher = pattern.matcher(name);
		if (fileNameMatcher.matches() && fileNameMatcher.groupCount() == 3) {
			try {
				return new CompaniesFileMetaData(name, directory,
						new SimpleDateFormat(DATE_FORMAT_GROUP).format(new SimpleDateFormat(DATE_FORMAT_FILE_NAME)
								.parse(fileNameMatcher.group(1))), Integer.parseInt(fileNameMatcher.group(2)),
						Integer.parseInt(fileNameMatcher.group(3)),
						new SimpleDateFormat(DATE_FORMAT_FILE_NAME).parse(fileNameMatcher.group(1)));
			} catch (Exception e) {
				throw new IOException("File name [" + name + "] could not be parsed, nested root cause follows", e);
			}
		} else {
			throw new IOException("File name [" + name + "] did not match regex specification ["
					+ pattern + "]");
		}
	}

	public static String parseGroup(String group) throws IOException {
		if (group == null) {
			throw new IllegalArgumentException("null group");
		}
		Date date = null;
		try {
			date = new SimpleDateFormat(DATE_FORMAT_GROUP).parse(group);
		} catch (ParseException exception) {
			throw new IOException("Could not parse group", exception);
		}
		return new SimpleDateFormat(DATE_FORMAT_GROUP_FILE).format(date).toUpperCase();
	}

	public static String[] parseRecord(String record) throws IOException {
		if (record == null) {
			throw new IllegalArgumentException("null record");
		}
		return FILE_RECORD_PARSER.parseLine(record);
	}

	@Override
	public String toString() {
		StringBuilder string = new StringBuilder();
		string.append("[name=");
		string.append(name);
		string.append(", directory=");
		string.append(directory);
		string.append(", group=");
		string.append(new SimpleDateFormat(DATE_FORMAT_GROUP).format(snapshotDate));
		string.append(", snapshotDate=");
		string.append(new SimpleDateFormat(DATE_FORMAT_FILE_NAME).format(snapshotDate));
		string.append(", part=");
		string.append(part);
		string.append(", partTotal=");
		string.append(partTotal);
		string.append("]");
		return string.toString();
	}

	private CompaniesFileMetaData(String name, String directory, String group, int part, int partTotal,
			Date snapshotDate) {
		super();
		this.name = name;
		this.directory = directory;
		this.group = group;
		this.part = part;
		this.partTotal = partTotal;
		this.snapshotDate = snapshotDate;
	}

	public String getName() {
		return name;
	}

	public String getDirectory() {
		return directory;
	}

	public String getGroup() {
		return group;
	}

	public int getPart() {
		return part;
	}

	public int getPartTotal() {
		return partTotal;
	}

	public Date getSnapshotDate() {
		return snapshotDate;
	}

}

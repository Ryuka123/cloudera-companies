package com.cloudera.companies.core.common;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import au.com.bytecode.opencsv.CSVParser;

public class CompaniesFileMetaData {

	public static final int FILE_FIELDS_NUMBER = 53;

	public static final String FILE_ESCAPE_CHAR = "@";
	public static final String FILE_ESCAPE_CHAR_ESCAPED = FILE_ESCAPE_CHAR + FILE_ESCAPE_CHAR;

	private static final String DATE_FORMAT_FILE_NAME = "yyyy-MM-dd";
	private static final String DATE_FORMAT_GROUP = "yyyy/MM";
	private static final String DATE_FORMAT_FILE_NAME_OUTPUT = "MMM-yyyy";

	private static final String FILE_NAME_REGEX_BASE = "BasicCompanyData-(20[0-9][0-9]-[0-1][0-9]-[0-3][0-9])-part([1-9])_([1-9])";
	private static final Pattern FILE_NAME_PATTERN_ZIP = Pattern.compile(FILE_NAME_REGEX_BASE + ".zip");
	private static final Pattern FILE_NAME_PATTERN_CSV = Pattern.compile(FILE_NAME_REGEX_BASE + ".csv");
	private static final Pattern FILE_NAME_PATTERN_REDUCE = Pattern.compile("([A-Z]{3}-20[0-9][0-9])-r-[0-9]{5}");

	private static final CSVParser FILE_RECORD_PARSER = new CSVParser(',', '"', FILE_ESCAPE_CHAR.charAt(0), true, true);

	private String name;
	private String directory;
	private String group;
	private int part;
	private int partTotal;
	private Date snapshotDate;

	public static CompaniesFileMetaData parsePathZip(String name, String directory) throws IOException {
		return parsePath(FILE_NAME_PATTERN_ZIP, DATE_FORMAT_FILE_NAME, name, directory);
	}

	public static CompaniesFileMetaData parsePathCSV(String name, String directory) throws IOException {
		return parsePath(FILE_NAME_PATTERN_CSV, DATE_FORMAT_FILE_NAME, name, directory);
	}

	public static CompaniesFileMetaData parsePathSeq(String name, String directory) throws IOException {
		return parsePath(FILE_NAME_PATTERN_REDUCE, DATE_FORMAT_FILE_NAME_OUTPUT, name, directory);
	}

	private static CompaniesFileMetaData parsePath(Pattern patternPath, String formatDate, String name, String directory)
			throws IOException {

		if (name == null) {
			throw new IllegalArgumentException("null name");
		}
		if (directory == null) {
			throw new IllegalArgumentException("null directory");
		}
		Matcher fileNameMatcher = patternPath.matcher(name);
		if (fileNameMatcher.matches()) {
			try {
				if (fileNameMatcher.groupCount() == 3) {
					return new CompaniesFileMetaData(name, directory, getDateFormat(DATE_FORMAT_GROUP).format(
							getDateFormat(formatDate).parse(fileNameMatcher.group(1))),
							Integer.parseInt(fileNameMatcher.group(2)), Integer.parseInt(fileNameMatcher.group(3)),
							getDateFormat(formatDate).parse(fileNameMatcher.group(1)));
				} else if (fileNameMatcher.groupCount() == 1) {
					return new CompaniesFileMetaData(name, directory, getDateFormat(DATE_FORMAT_GROUP).format(
							getDateFormat(formatDate).parse(fileNameMatcher.group(1))), 1, 1, getDateFormat(formatDate)
							.parse(fileNameMatcher.group(1)));
				}

			} catch (Exception e) {
				throw new IOException("File name [" + name + "] could not be parsed, nested root cause follows", e);
			}
		}
		throw new IOException("File name [" + name + "] did not match regex specification [" + patternPath + "]");
	}

	public static Date parsePathGroup(String group) throws IOException {
		if (group == null) {
			throw new IllegalArgumentException("null group");
		}
		Date date = null;
		try {
			date = getDateFormat(DATE_FORMAT_GROUP).parse(group);
		} catch (ParseException exception) {
			throw new IOException("Could not parse group", exception);
		}
		return date;
	}

	public static String parsePathGroupFile(String group) throws IOException {
		return getDateFormat(DATE_FORMAT_FILE_NAME_OUTPUT).format(parsePathGroup(group)).toUpperCase();
	}

	public static String[] parseRecord(String record) throws IOException {
		// Investigate using faster, single pass parsing routine, without the
		// need to pre-escape escape sequences
		return FILE_RECORD_PARSER.parseLine(record.replace(FILE_ESCAPE_CHAR, FILE_ESCAPE_CHAR_ESCAPED));
	}

	private static DateFormat getDateFormat(String format) {
		DateFormat dateFormat = new SimpleDateFormat(format);
		dateFormat.setTimeZone(CompaniesConstants.DATE_TIMEZONE);
		return dateFormat;
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

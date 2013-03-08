package com.cloudera.companies.core.ingest;

import java.io.IOException;

import com.cloudera.companies.core.common.CompaniesFileMetaData;

public class IngestUtil {

  public enum Counter {

    // Dataset counters
    DATASETS, DATASETS_SUCCESS, DATASETS_SKIP, DATASETS_FAILURE,

    // File action counters
    FILES, FILES_SUCCESS, FILES_SKIP, FILES_FAILURE,

    // File status counters
    FILES_VALID, FILES_ERROR, FILES_PARTIAL, FILES_UNKNOWN, FILES_CLEANED,

    // Record action counters
    RECORDS, RECORDS_VALID("cleansed/"), RECORDS_MALFORMED("erroneous/malformed/"), RECORDS_DUPLICATE(
        "erroneous/duplicate/");

    private String path;

    Counter() {
    }

    Counter(String path) {
      this.path = path;
    }

    public String getPath() {
      return path == null ? "" : path;
    }

  };

  public static boolean isNamespacedFile(String file, String directory, String group, Counter counter) {
    try {
      return isNamespacedPath(group, counter)
          && (counter != null && (counter.equals(Counter.FILES_SUCCESS) ? CompaniesFileMetaData.parsePathZip(file,
              directory) != null : CompaniesFileMetaData.parsePathSeq(file, directory) != null));
    } catch (Exception e) {
      return false;
    }
  }

  public static boolean isNamespacedPath(String group, Counter counter) {
    try {
      if (counter == null) {
        return CompaniesFileMetaData.parsePathGroup(group) != null;
      } else {
        return CompaniesFileMetaData.parsePathGroup(group.replaceFirst(counter.getPath(), "")) != null;
      }
    } catch (Exception e) {
      return false;
    }
  }

  public static String getNamespacedPath(Counter counter, String group) throws IOException {
    return counter.getPath() + (group == null ? "" : group);
  }

  public static String getNamespacedPathFile(Counter counter, String group) throws IOException {
    return getNamespacedPath(counter, group) + "/"
        + (group == null ? "data" : CompaniesFileMetaData.parsePathGroupFile(group));
  }

}

package com.cloudera.companies.core.ingest.seq.mr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.cloudera.companies.core.common.CompaniesFileMetaData;

public class CompaniesFileZipFileInputFormat extends FileInputFormat<Text, Text> {

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  @Override
  public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    return new RecordReader<Text, Text>() {

      private Path filePath;

      private FSDataInputStream inputStreamFs;
      private ZipInputStream inputStreamZip;
      private InputStreamReader inputSteamReader;
      private BufferedReader bufferedReader;

      private CompaniesFileMetaData fileMetaData;

      private Text currentKey;
      private String currentValue;

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        filePath = ((FileSplit) split).getPath();
        inputStreamZip = new ZipInputStream(inputStreamFs = filePath.getFileSystem(context.getConfiguration()).open(
            filePath));

        if ((fileMetaData = nextFileMetaData()) == null) {
          throw new IOException("Could not find suitably named file in table of contents of zip file [" + filePath
              + "]");
        }

        currentKey = new Text(fileMetaData.getGroup());
        bufferedReader = new BufferedReader(inputSteamReader = new InputStreamReader(inputStreamZip));
      }

      private CompaniesFileMetaData nextFileMetaData() throws IOException {

        ZipEntry fileZipEntry = null;
        CompaniesFileMetaData fileMetaData = null;
        while ((fileZipEntry = inputStreamZip.getNextEntry()) != null) {
          try {
            fileMetaData = CompaniesFileMetaData.parsePathCSV(new Path(fileZipEntry.getName()).getName(), filePath
                .getParent().toString());
            break;
          } catch (IOException ignore) {
          }
        }

        return fileMetaData;
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        // skip header
        if (currentValue == null) {
          bufferedReader.readLine();
        }
        return (currentValue = bufferedReader.readLine()) != null;
      }

      @Override
      public Text getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
      }

      @Override
      public Text getCurrentValue() throws IOException, InterruptedException {
        return new Text(currentValue);
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        return currentValue == null ? 1 : 0;
      }

      @Override
      public void close() throws IOException {
        inputStreamZip.closeEntry();
        bufferedReader.close();
        inputSteamReader.close();
        inputStreamZip.close();
        inputStreamFs.close();
      }

    };
  }
}

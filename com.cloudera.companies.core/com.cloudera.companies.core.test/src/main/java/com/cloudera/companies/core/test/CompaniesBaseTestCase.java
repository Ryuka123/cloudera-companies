package com.cloudera.companies.core.test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

public abstract class CompaniesBaseTestCase implements CompaniesBaseTest {

  static {
    System.setProperty("java.security.krb5.realm", "CDHCLUSTER.com");
    System.setProperty("java.security.krb5.kdc", "kdc.cdhcluster.com");

    System.setProperty("derby.stream.error.file", CompaniesBaseTest.PATH_LOCAL_WORKING_DIR + "/target/derby.log");

    System.setProperty("dir.working", CompaniesBaseTest.PATH_LOCAL_WORKING_DIR);
    System.setProperty("dir.working.target", CompaniesBaseTest.PATH_LOCAL_WORKING_DIR_TARGET);
    System.setProperty("dir.working.target.hdfs", CompaniesBaseTest.PATH_LOCAL_WORKING_DIR_TARGET_HDFS);
  }

  public static void initHadoopHome(String hadoopHome) {
    Map<String, String> env = new HashMap<String, String>();
    env.putAll(System.getenv());
    if (System.getenv(ENV_HADOOP_HOME) == null) {
      File target = new File(hadoopHome);
      if (target.exists()) {
        env.put(ENV_HADOOP_HOME, target.getAbsolutePath());
        CompaniesBaseTestCase.setEnvionment(env);
      }
    }
  }

  public static void init() {
    try {
      Thread.sleep(1000);
    } catch (Exception exception) {
      throw new RuntimeException("Could not construct test", exception);
    }
    initHadoopHome(PATH_HADOOP_HOME);
  }

  public static void setUp(FileSystem fileSystem) throws IOException {
    if (fileSystem != null) {
      Path rootPath = new Path(CompaniesBaseTestCase.getPathHDFS("/"));
      Path tmpPath = new Path(CompaniesBaseTestCase.getPathHDFS("/tmp"));
      fileSystem.delete(rootPath, true);
      fileSystem.mkdirs(rootPath);
      fileSystem.mkdirs(tmpPath);
      fileSystem.setPermission(tmpPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    }
  }

  public static void tearDown(FileSystem fileSystem) throws IOException {
    fileSystem.close();
  }

  public static String getPathLocal(String pathRelativeToModuleRoot) {
    String pathRelativeToModuleRootLessLeadingSlashes = stripLeadingSlashes(pathRelativeToModuleRoot);
    return pathRelativeToModuleRootLessLeadingSlashes.equals("") ? (PATH_LOCAL_WORKING_DIR.length() < 2 ? "/"
        : PATH_LOCAL_WORKING_DIR.substring(0, PATH_LOCAL_WORKING_DIR.length() - 2)) : new Path(PATH_LOCAL_WORKING_DIR,
        pathRelativeToModuleRootLessLeadingSlashes).toUri().toString();
  }

  public static String getPathHDFS(String pathRelativeToHDFSRoot) {
    String pathRelativeToHDFSRootLessLeadingSlashes = stripLeadingSlashes(pathRelativeToHDFSRoot);
    return pathRelativeToHDFSRootLessLeadingSlashes.equals("") ? PATH_HDFS : new Path(PATH_HDFS,
        pathRelativeToHDFSRootLessLeadingSlashes).toUri().toString();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static void setEnvionment(Map<String, String> newenv) {
    try {
      Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
      Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
      theEnvironmentField.setAccessible(true);
      Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
      env.putAll(newenv);
      Field theCaseInsensitiveEnvironmentField = processEnvironmentClass
          .getDeclaredField("theCaseInsensitiveEnvironment");
      theCaseInsensitiveEnvironmentField.setAccessible(true);
      Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
      cienv.putAll(newenv);
    } catch (NoSuchFieldException e) {
      try {
        Class[] classes = Collections.class.getDeclaredClasses();
        Map<String, String> env = System.getenv();
        for (Class cl : classes) {
          if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
            Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            Object obj = field.get(env);
            Map<String, String> map = (Map<String, String>) obj;
            map.clear();
            map.putAll(newenv);
          }
        }
      } catch (Exception excpetion2) {
        // ignore
      }
    } catch (Exception expcetion1) {
      // ignore
    }
  }

  private static String stripLeadingSlashes(String string) {
    int indexAfterLeadingSlash = 0;
    while (indexAfterLeadingSlash < string.length() && string.charAt(indexAfterLeadingSlash) == '/')
      ++indexAfterLeadingSlash;
    return indexAfterLeadingSlash == 0 ? string : string.substring(indexAfterLeadingSlash, string.length());
  }

}

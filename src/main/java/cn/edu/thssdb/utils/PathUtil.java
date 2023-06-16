package cn.edu.thssdb.utils;

import java.io.File;

public class PathUtil {

  public static String getDBFolderPath(String databaseName) {
    return Global.DATA_PATH + File.separator + databaseName + File.separator;
  }

  public static String getBinFilePath(String databaseName, String tableName) {
    return Global.DATA_PATH
        + File.separator
        + databaseName
        + File.separator
        + tableName
        + File.separator
        + tableName
        + ".bin";
  }

  public static String getTableFolderPath(String databaseName, String tableName) {
    return Global.DATA_PATH
        + File.separator
        + databaseName
        + File.separator
        + tableName
        + File.separator;
  }

  public static String getMetaFilePath(String databaseName, String tableName) {
    return Global.DATA_PATH
        + File.separator
        + databaseName
        + File.separator
        + tableName
        + File.separator
        + "_meta";
  }
}

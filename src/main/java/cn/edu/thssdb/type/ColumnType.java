package cn.edu.thssdb.type;

import cn.edu.thssdb.exception.IllegalTypeException;

public enum ColumnType {
  INT,
  LONG,
  FLOAT,
  DOUBLE,
  STRING;

  public static Comparable getColumnTypeValue(ColumnType c, String val) {
    if (val.equalsIgnoreCase("null")) return null;
    try {
      if (c == INT) return Double.valueOf(val).intValue();
      else if (c == LONG) return Double.valueOf(val).longValue();
      else if (c == FLOAT) return Double.valueOf(val).floatValue();
      else if (c == DOUBLE) return Double.valueOf(val).doubleValue();
      else if (c == STRING) return val;
      else throw new IllegalTypeException();
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }
}

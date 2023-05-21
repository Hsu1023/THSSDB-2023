package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.NullValueException;
import cn.edu.thssdb.exception.ValueExceedException;
import cn.edu.thssdb.type.ColumnType;

public class Column implements Comparable<Column> {
  private String name;
  private ColumnType type;
  private int primary;
  private boolean notNull;
  private int maxLength;

  public Column(String name, ColumnType type, int primary, boolean notNull, int maxLength) {
    this.name = name;
    this.type = type;
    this.primary = primary;
    this.notNull = notNull;
    this.maxLength = maxLength;
  }

  @Override
  public int compareTo(Column e) {
    return name.compareTo(e.name);
  }

  public String toString() {
    return name + ',' + type + ',' + primary + ',' + notNull + ',' + maxLength;
  }

  public static Column toColumn(String str){
    String[] strings = str.split(",");
    return new Column(strings[0], ColumnType.valueOf(strings[1]), Integer.valueOf(strings[2]),
            Boolean.valueOf(strings[3]), Integer.valueOf(strings[4]));

  }

  public String getName() {
    return name;
  }

  public void setPrimary(int flag) {
    primary = flag;
  }

  public String getColumnName() {
    return this.name;
  }

  public ColumnType getColumnType() {
    return this.type;
  }

  public boolean isPrimary() {
    return this.primary == 1;
  }

  public boolean cantBeNull() {
    return this.notNull;
  }

  public int getMaxLength() {
    return this.maxLength;
  }

  public Entry parseEntry(String valueString) {
    ColumnType columnType = getColumnType();
    if (valueString.toLowerCase().equals("null")) {
      if (cantBeNull()) throw new NullValueException(getColumnName()); // 该列不可为null
      else {
        return new Entry(null);
      }
    }
    switch (columnType) {
      case INT:
        return new Entry(Integer.valueOf(valueString));
      case LONG:
        return new Entry(Long.valueOf(valueString));
      case FLOAT:
        return new Entry(Float.valueOf(valueString));
      case DOUBLE:
        return new Entry(Double.valueOf(valueString));
      case STRING:
        String sWithoutQuotes = valueString.substring(1, valueString.length() - 1);
        if (sWithoutQuotes.length() > getMaxLength()) // 长度超出该列限制
        throw new ValueExceedException(
              getColumnName(), valueString.length(), getMaxLength(), "(when parse row)");
        return new Entry(sWithoutQuotes);
      default:
        return new Entry(null);
    }
  }
}

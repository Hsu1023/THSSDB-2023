package cn.edu.thssdb.schema;

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
}

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

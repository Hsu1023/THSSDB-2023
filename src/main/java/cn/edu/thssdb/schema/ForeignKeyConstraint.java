package cn.edu.thssdb.schema;

public class ForeignKeyConstraint {
  Table foreignTable;
  Column localColumn;
  Column foreignTablePrimaryColumn;

  public ForeignKeyConstraint(
      Table foreignTable, Column localColumn, Column foreignTablePrimaryColumn) {
    this.foreignTable = foreignTable;
    this.localColumn = localColumn;
    this.foreignTablePrimaryColumn = foreignTablePrimaryColumn;
  }

  boolean check(Table localTable, Row row) {
    int localIndex = localTable.columns.indexOf(localColumn);
    Entry value = row.getEntries().get(localIndex);
    return foreignTable.index.contains(value);
  }
}

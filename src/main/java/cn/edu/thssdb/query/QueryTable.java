package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.AttributeNotExistException;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.sql.SQLParser;

import java.util.ArrayList;
import java.util.Iterator;

public class QueryTable implements Iterator<Row> {
  public ArrayList<Column> columns;
  public ArrayList<Row> rows;

  QueryTable() {
    // TODO
  }

  public QueryTable(Table table) {
    this.columns = new ArrayList<>();
    for (Column column : table.columns) {
      Column new_column =
          new Column(
              table.tableName + "." + column.getColumnName(),
              column.getColumnType(),
              column.getPrimary(),
              column.cantBeNull(),
              column.getMaxLength());
      this.columns.add(new_column);
    }
    Iterator<Row> rowIterator = table.iterator();
    this.rows = QueryTable.getRowsSatisfyWhereClause(rowIterator, columns, null);
  }

  @Override
  public boolean hasNext() {
    // TODO
    return true;
  }

  @Override
  public Row next() {
    // TODO
    return null;
  }

  public static QueryTable getQueryTableFromSingleTable(String tableName) {
    Database database = Manager.getInstance().getAndAssumeCurrentDatabase();
    Table table = database.get(tableName);
    QueryTable queryTable = new QueryTable(table);
    return queryTable;
  }

  public static QueryTable fromQueryCtx(SQLParser.TableQueryContext ctx) {
    if (ctx.getChildCount() == 1) {
      return QueryTable.getQueryTableFromSingleTable(ctx.tableName(0).getText());
    }

    // TODO: multi-table query
    return new QueryTable();
  }

  public static int getIndexOfAttrName(ArrayList<Column> columns, String AttrName) {
    for (int i = 0; i < columns.size(); ++i) {
      if (columns.get(i).getColumnName().equals(AttrName)) {
        return i;
      }
    }
    for (int i = 0; i < columns.size(); ++i) {
      String columnName = columns.get(i).getColumnName().split("\\.", 2)[1]; // student.name => name
      if (columnName.equals(AttrName)) {
        return i;
      }
    }
    return -1;
  }

  public static ArrayList<Row> getRowsSatisfyWhereClause(
      Iterator<Row> rowIterator,
      ArrayList<Column> columns,
      SQLParser.ConditionContext updateCondition) {

    String attrName = null;
    String attrValue = null;
    int attrIndex = 0;
    SQLParser.ComparatorContext comparator = null;
    Entry compareValue = null;
    ArrayList<Row> rows = new ArrayList<Row>();

    if (updateCondition != null) {
      attrName =
          updateCondition
              .expression(0)
              .comparer()
              .columnFullName()
              .columnName()
              .getText()
              .toLowerCase();
      attrValue = updateCondition.expression(1).comparer().literalValue().getText();
      attrIndex = getIndexOfAttrName(columns, attrName);
      comparator = updateCondition.comparator();
      if (attrIndex == -1) {
        throw new AttributeNotExistException();
      }
      compareValue = columns.get(attrIndex).parseEntry(attrValue);
    }

    while (rowIterator.hasNext()) {
      Row row = rowIterator.next();
      Entry columnValue = row.getEntries().get(attrIndex);
      boolean flag = false;
      if (comparator == null) {
        flag = true;
      } else if (columnValue.value == null || compareValue.value == null) {
        flag = false;
      } else if (comparator.LT() != null) {
        if (columnValue.compareTo(compareValue) < 0) flag = true;
      } else if (comparator.GT() != null) {
        if (columnValue.compareTo(compareValue) > 0) flag = true;
      } else if (comparator.LE() != null) {
        if (columnValue.compareTo(compareValue) <= 0) flag = true;
      } else if (comparator.GE() != null) {
        if (columnValue.compareTo(compareValue) >= 0) flag = true;
      } else if (comparator.EQ() != null) {
        if (columnValue.compareTo(compareValue) == 0) flag = true;
      } else if (comparator.NE() != null) {
        if (columnValue.compareTo(compareValue) != 0) flag = true;
      }

      if (flag) {
        rows.add(row);
      }
    }
    return rows;
  }

  public void filteredOnColumns(ArrayList<Integer> columnIndexs) {
    ArrayList<Row> newRows = new ArrayList<Row>();
    ArrayList<Column> newColumns = new ArrayList<Column>();

    Iterator<Row> rowIterator = rows.iterator();
    while (rowIterator.hasNext()) {
      Row row = rowIterator.next();
      ArrayList<Entry> finalRowEntries = new ArrayList<>();
      for (int index : columnIndexs) {
        finalRowEntries.add(row.getEntries().get(index));
      }
      newRows.add(new Row(finalRowEntries));
    }

    for (int index : columnIndexs) newColumns.add(columns.get(index));

    rows = newRows;
    columns = newColumns;
  }

  public QueryResult toQueryResult() {
    ArrayList<String> columnNameList = new ArrayList<>();
    for (Column column : columns) columnNameList.add(column.getName());
    return new QueryResult(rows, columnNameList);
  }

  public Iterator<Row> iterator() {
    return rows.iterator();
  }
}

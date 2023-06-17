package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.AttributeNotExistException;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.index.BPlusTreeIterator;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.utils.Pair;

import java.util.ArrayList;
import java.util.Iterator;

public class QueryTable implements Iterator<Row> {
  public ArrayList<Column> columns;
  public ArrayList<Row> rows;
  public BPlusTree<Entry, Row> index;

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
    this.index = table.index;
  }

  public QueryTable(
      QueryTable left_table, QueryTable right_table, SQLParser.ConditionContext joinCondition) {
    (this.columns = new ArrayList<>(left_table.columns)).addAll(right_table.columns);
    this.rows = new ArrayList<>();

    String leftColumnName = null, rightColumnName = null;
    int leftColumnIndex = -1, rightColumnIndex = -1;
    //    if (joinCondition == null) {
    //      // throw wrong join condition exception
    //      System.out.println("join condition is null");
    //    }
    // 获取join条件下左右两列在各自表中的名字和index
    leftColumnName = joinCondition.expression(0).getText();
    rightColumnName = joinCondition.expression(1).getText();
    leftColumnIndex = QueryTable.getIndexOfAttrName(left_table.columns, leftColumnName);
    rightColumnIndex = QueryTable.getIndexOfAttrName(right_table.columns, rightColumnName);

    if (leftColumnIndex == -1 || rightColumnIndex == -1) {
      throw new AttributeNotExistException();
    }

    // when join on primary key
    if (left_table.columns.get(leftColumnIndex).isPrimary()
        && right_table.columns.get(rightColumnIndex).isPrimary()) {
      BPlusTreeIterator<Entry, Row> left_table_iter = left_table.index.iterator();
      BPlusTreeIterator<Entry, Row> right_table_iter = right_table.index.iterator();

      if (!right_table_iter.hasNext() || !left_table_iter.hasNext()) return;

      Pair<Entry, Row> right_table_element = right_table_iter.next();
      while (left_table_iter.hasNext()) {
        Pair<Entry, Row> left_table_element = left_table_iter.next();
        while (right_table_iter.hasNext()
            && left_table_element.left.compareTo(right_table_element.left) > 0) // left > right
        right_table_element = right_table_iter.next();

        if (left_table_element.left.equals(right_table_element.left)) {
          Row new_row = new Row(left_table_element.right.getEntries());
          new_row.getEntries().addAll(right_table_element.right.getEntries());
          this.rows.add(new_row);
        }
      }

      return;
    }

    for (Row left_row : left_table.rows) {
      for (Row right_row : right_table.rows) {
        Entry leftRefValue = left_row.getEntries().get(leftColumnIndex);
        Entry rightRefValue = right_row.getEntries().get(rightColumnIndex);
        if (leftRefValue.equals(rightRefValue) == false) {
          continue;
        }
        Row new_row = new Row(left_row.getEntries());
        new_row.getEntries().addAll(right_row.getEntries());
        this.rows.add(new_row);
      }
    }
  }

  // 笛卡尔积
  public QueryTable(QueryTable left_table, QueryTable right_table) {
    (this.columns = new ArrayList<>(left_table.columns)).addAll(right_table.columns);
    this.rows = new ArrayList<>();

    for (Row left_row : left_table.rows) {
      for (Row right_row : right_table.rows) {
        Row new_row = new Row(left_row.getEntries());
        new_row.getEntries().addAll(right_row.getEntries());
        this.rows.add(new_row);
      }
    }
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
    return QueryTable.getQueryTableFromSingleTable(ctx.tableName(0).getText());
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
      attrName = updateCondition.expression(0).comparer().columnFullName().columnName().getText();
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

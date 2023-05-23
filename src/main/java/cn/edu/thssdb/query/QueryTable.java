package cn.edu.thssdb.query;

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

  public static ArrayList<Row> getRowsSatisfyWhereClause(
      Iterator<Row> rowIterator,
      ArrayList<Column> columns,
      SQLParser.ConditionContext updateCondition) {

    // TODO: Partial Columns
    // TODO: Condition Query
    ArrayList<Row> rows = new ArrayList<Row>();
    while (rowIterator.hasNext()) {
      Row row = rowIterator.next();
      rows.add(row);
    }
    return rows;
  }
}

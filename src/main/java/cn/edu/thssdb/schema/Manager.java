package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Global;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
  private HashMap<String, Database> databases;

  private Database curDatabase = null;

  private static String MANAGER_DATAPATH = Global.DATA_PATH + File.separator + "manager.db";
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    // TODO
    databases = new HashMap<>();
    curDatabase = null;
    File dataFile = new File(MANAGER_DATAPATH);
    if (!dataFile.exists()) {
      dataFile.getParentFile().mkdirs();
    }
    this.recover();
  }

  //  private Database GetCurrentDB() {
  //    if(curDatabase == null) {
  //      throw new DatabaseNotExistException();
  //    }
  //    return curDatabase;
  //  }

  public void createDatabaseIfNotExists(String name) {
    // TODO
    Boolean change = false;
    lock.writeLock().lock();
    try {
      if (!databases.containsKey(name)) {
        Database newDatabase = new Database(name);
        databases.put(name, newDatabase);
        change = true;
        System.out.println("[DEBUG] " + "create db " + name);
      } else {
        System.out.println("[DEBUG] " + "duplicated db " + name);
        throw new DuplicateDatabaseException();
      }
    } finally {
      if (change) persistDatabases();
      lock.writeLock().unlock();
    }
  }

  public void deleteDatabase(String name) {

    Boolean change = false;
    lock.writeLock().lock();
    try {
      if (databases.containsKey(name)) {
        Database db = databases.get(name);
        databases.remove(name);
        change = true;
        System.out.println("[DEBUG] " + "delete db " + name);
      } else {
        System.out.println("[DEBUG] " + "non-existed db " + name);
        throw new DatabaseNotExistException();
      }
    } finally {
      if (change) persistDatabases();
      lock.writeLock().unlock();
    }
  }

  public ArrayList<String> getDatabaseNames() {
    return new ArrayList<>(databases.keySet());
  }

  public void switchDatabase(String name) {
    try {
      lock.readLock().lock();
      if (!databases.containsKey(name)) {
        System.out.println("[DEBUG] " + "non-existed db " + name);
        throw new DatabaseNotExistException();
      }
      curDatabase = databases.get(name);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void persistDatabases() {
    // without lock
    try {
      FileOutputStream fos = new FileOutputStream(MANAGER_DATAPATH);
      OutputStreamWriter writer = new OutputStreamWriter(fos);
      for (String databaseName : databases.keySet()) writer.write(databaseName + "\n");
      writer.close();
      fos.close();
    } catch (Exception e) {
      e.printStackTrace();
      // throw exception
    }
  }

  public void quit() {
    try {
      for (String i : databases.keySet()) {
        Database database = databases.get(i);
        for (String j : database.tables.keySet()) {
          Table table = database.tables.get(j);
          table.persist();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void createTableIfNotExist(SQLParser.CreateTableStmtContext ctx) {
    try {
      Database database = getAndAssumeCurrentDatabase();
      String tableName = ctx.tableName().children.get(0).toString(); // create table tableName
      int n = ctx.getChildCount();
      ArrayList<Column> columnItems = new ArrayList<>();
      //    for (int i = 0; i < n; i += 1) {
      //      System.out.println(ctx.getChild(i));
      //    }
      for (int i = 4; i < n; i += 2) { // 对每个数据项的type进行分析
        // 如果是普通数据项
        if (ctx.getChild(i)
            .getClass()
            .getName()
            .equals("cn.edu.thssdb.sql.SQLParser$ColumnDefContext")) {
          // 抽出数据项名字
          String columnName =
              ((SQLParser.ColumnDefContext) ctx.getChild(i))
                  .columnName()
                  .children
                  .get(0)
                  .toString()
                  .toLowerCase(Locale.ROOT);
          // 抽出数据项类别
          String typeName =
              ((SQLParser.ColumnDefContext) ctx.getChild(i)).typeName().children.get(0).toString();
          // 分析数据项类别
          ColumnType type = ColumnType.INT;
          if (typeName.toLowerCase().equals("int")) {
            type = ColumnType.INT;
          } else if (typeName.toLowerCase().equals("long")) {
            type = ColumnType.LONG;
          } else if (typeName.toLowerCase().equals("float")) {
            type = ColumnType.FLOAT;
          } else if (typeName.toLowerCase().equals("double")) {
            type = ColumnType.DOUBLE;
          } else if (typeName.toLowerCase().equals("string")) {
            type = ColumnType.STRING;
          }
          // 分析类别长度限制
          int length = 128;
          try {
            length =
                Integer.parseInt(
                    ((SQLParser.ColumnDefContext) ctx.getChild(i))
                        .typeName()
                        .children
                        .get(2)
                        .toString());
          } catch (Exception e) {

          }
          // 分析类别Not Null限制
          Boolean notNull = false;
          int constraint_num =
              ((SQLParser.ColumnDefContext) ctx.getChild(i)).columnConstraint().size();
          for (int j = 0; j < constraint_num; j++) {
            if (((SQLParser.ColumnDefContext) ctx.getChild(i))
                    .columnConstraint(j)
                    .children
                    .get(0)
                    .toString()
                    .toLowerCase()
                    .equals("not")
                && ((SQLParser.ColumnDefContext) ctx.getChild(i))
                    .columnConstraint(j)
                    .children
                    .get(1)
                    .toString()
                    .toLowerCase()
                    .equals("null")) {
              notNull = true;
            }
          }
          columnItems.add(new Column(columnName, type, 0, notNull, length)); // 新增column Item
        }
        // 如果是primary key约束项
        else {
          if (((SQLParser.TableConstraintContext) ctx.getChild(i))
                  .children
                  .get(0)
                  .toString()
                  .toLowerCase()
                  .equals("primary")
              && ((SQLParser.TableConstraintContext) ctx.getChild(i))
                  .children
                  .get(1)
                  .toString()
                  .toLowerCase()
                  .equals("key")) {

            ArrayList<String> primaryKeys = new ArrayList<>();
            int primaryKeyNum = ctx.getChild(i).getChildCount();
            for (int j = 3; j < primaryKeyNum; j += 2) {
              String columnName =
                  ((SQLParser.ColumnNameContext)
                          (((SQLParser.TableConstraintContext) ctx.getChild(i)).children.get(j)))
                      .children
                      .get(0)
                      .toString()
                      .toLowerCase(Locale.ROOT);
              primaryKeys.add(columnName);
            }
            //              System.out.println(primaryKeys);

            int columnNum = columnItems.size();
            for (int j = 0; j < columnNum; j++) {
              if (primaryKeys.contains(columnItems.get(j).getName())) {
                columnItems.get(j).setPrimary(1);
              }
            }
          }
        }
      }
      database.create(tableName, columnItems.toArray(new Column[columnItems.size()]));
    } finally {

    }
  }

  public Database getAndAssumeCurrentDatabase() {
    if (curDatabase == null) {
      System.out.println("[DEBUG] " + "current db is null");
      throw new DatabaseNotExistException();
    }
    return curDatabase;
  }

  public void insert(SQLParser.InsertStmtContext ctx) {
    try {
      // get table
      Database database = getAndAssumeCurrentDatabase();
      String tableName = ctx.tableName().children.get(0).toString();
      if (!curDatabase.tables.containsKey(tableName)) {
        throw new TableNotExistException();
      }
      Table table = database.get(tableName);

      // get value list
      List<SQLParser.ValueEntryContext> values = ctx.valueEntry();
      ArrayList<String> valueStringList = new ArrayList<>();
      for (SQLParser.ValueEntryContext value : values) {
        for (int i = 0; i < value.literalValue().size(); i++) {
          String valueString = value.literalValue(i).getText();
          valueStringList.add(valueString);
        }
      }

      // get selected columns (match insert names of columns and table columns)
      List<SQLParser.ColumnNameContext> columnName = ctx.columnName();
      ArrayList<Column> allColumns = table.columns;
      ArrayList<Column> selectedColumns = new ArrayList<>();
      if (columnName.size() == 0) {
        selectedColumns = new ArrayList<>(allColumns.subList(0, valueStringList.size()));
      } else {
        for (int i = 0; i < columnName.size(); i++) {
          for (int j = 0; j < allColumns.size(); j++) {
            if (columnName.get(i).getText().equals(allColumns.get(j).getColumnName())) {
              selectedColumns.add(allColumns.get(j));
              break;
            }
          }
        }
      }

      if (valueStringList.size() != selectedColumns.size()) {
        throw new SchemaLengthMismatchException(
            selectedColumns.size(),
            valueStringList.size(),
            "wrong insert operation (columns unmatched)!");
      }

      ArrayList<Entry> entries = new ArrayList<>();
      System.out.println(allColumns);
      System.out.println(selectedColumns);
      for (Column column : allColumns) {
        int id = selectedColumns.indexOf(column);
        System.out.println(id);
        String valueString = "NULL";
        if (id != -1) valueString = valueStringList.get(id);
        Entry entry = column.parseEntry(valueString);
        entries.add(entry);
      }
      Row newRow = new Row(entries);
      table.insert(newRow);
      System.out.println("[DEBUG]" + "current number of rows is " + table.getRowSize());
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
      // throw exception
    }
  }

  public void delete(SQLParser.DeleteStmtContext ctx) {
    try {
      // get table
      Database database = getAndAssumeCurrentDatabase();
      String tableName = ctx.tableName().children.get(0).toString();
      if (!database.tables.containsKey(tableName)) {
        throw new TableNotExistException();
      }
      Table table = database.get(tableName);
      ArrayList<Column> columns = table.columns;

      Iterator<Row> rowIterator = table.iterator();

      if (ctx.K_WHERE() == null) {
        while (rowIterator.hasNext()) {
          Row curRow = rowIterator.next();
          table.delete(curRow);
        }
      } else {
        SQLParser.ConditionContext condition = ctx.multipleCondition().condition();
        SQLParser.ComparerContext attrComparer = condition.expression(0).comparer();
        String attr = attrComparer.columnFullName().columnName().getText().toLowerCase();
        SQLParser.ComparatorContext comparator = condition.comparator();
        SQLParser.ComparerContext valueComparer = condition.expression(1).comparer();
        String value = valueComparer.literalValue().getText();
        int columnIndex = -1;
        Column curColumn = null;
        for (int i = 0; i < columns.size(); i++)
          if (columns.get(i).getName().equals(attr)) {
            columnIndex = i;
            curColumn = columns.get(i);
            break;
          }
        if (columnIndex == -1) {
          throw new AttributeNotExistException();
        }

        Entry comparedEntry = curColumn.parseEntry(value);
        while (rowIterator.hasNext()) {
          Row curRow = rowIterator.next();
          Entry curEntry = curRow.getEntries().get(columnIndex);
          if ((comparator.EQ() != null && comparedEntry.compareTo(curEntry) == 0)
              || (comparator.NE() != null && comparedEntry.compareTo(curEntry) != 0)
              || (comparator.GE() != null && comparedEntry.compareTo(curEntry) >= 0)
              || (comparator.LE() != null && comparedEntry.compareTo(curEntry) <= 0)
              || (comparator.GT() != null && comparedEntry.compareTo(curEntry) < 0)
              || (comparator.LT() != null && comparedEntry.compareTo(curEntry) > 0))
            table.delete(curRow);
        }
      }

      System.out.println("[DEBUG]" + "current number of rows is " + table.getRowSize());
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void update(SQLParser.UpdateStmtContext ctx) {
    try {
      // get table
      Database database = getAndAssumeCurrentDatabase();
      String tableName = ctx.tableName().children.get(0).toString();
      if (!database.tables.containsKey(tableName)) {
        throw new TableNotExistException();
      }
      Table table = database.get(tableName);
      ArrayList<Column> columns = table.columns;

      Iterator<Row> rowIterator = table.iterator();

      String columnName = ctx.columnName().getText();
      int updateIndex = table.getColumnIndexByName(columnName);
      Column selectedColumn = columns.get(updateIndex);
      Entry attrValue =
          selectedColumn.parseEntry(ctx.expression().comparer().literalValue().getText());

      if (ctx.K_WHERE() == null) {
        while (rowIterator.hasNext()) {
          Row curRow = rowIterator.next();
          ArrayList<Entry> oldRowEntries = new ArrayList<>(curRow.getEntries());
          oldRowEntries.set(updateIndex, attrValue);
          Row newRow = new Row(oldRowEntries);
          table.update(curRow, newRow);
        }
      } else {
        SQLParser.ConditionContext condition = ctx.multipleCondition().condition();
        SQLParser.ComparerContext attrComparer = condition.expression(0).comparer();
        String attr = attrComparer.columnFullName().columnName().getText().toLowerCase();
        SQLParser.ComparatorContext comparator = condition.comparator();
        SQLParser.ComparerContext valueComparer = condition.expression(1).comparer();
        String value = valueComparer.literalValue().getText();
        int columnIndex = -1;
        Column curColumn = null;
        for (int i = 0; i < columns.size(); i++)
          if (columns.get(i).getName().equals(attr)) {
            columnIndex = i;
            curColumn = columns.get(i);
            break;
          }
        if (columnIndex == -1) {
          throw new AttributeNotExistException();
        }

        Entry comparedEntry = curColumn.parseEntry(value);
        while (rowIterator.hasNext()) {
          Row curRow = rowIterator.next();
          Entry curEntry = curRow.getEntries().get(columnIndex);
          if ((comparator.EQ() != null && comparedEntry.compareTo(curEntry) == 0)
              || (comparator.NE() != null && comparedEntry.compareTo(curEntry) != 0)
              || (comparator.GE() != null && comparedEntry.compareTo(curEntry) >= 0)
              || (comparator.LE() != null && comparedEntry.compareTo(curEntry) <= 0)
              || (comparator.GT() != null && comparedEntry.compareTo(curEntry) < 0)
              || (comparator.LT() != null && comparedEntry.compareTo(curEntry) > 0)) {
            ArrayList<Entry> oldRowEntries = new ArrayList<>(curRow.getEntries());
            oldRowEntries.set(updateIndex, attrValue);
            Row newRow = new Row(oldRowEntries);
            table.update(curRow, newRow);
          }
        }
      }
      System.out.println("[DEBUG]" + "current number of rows is " + table.getRowSize());
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  public QueryResult select(SQLParser.SelectStmtContext ctx) {
    try {
      // TODO: lock, from multiple tables

      // from TABLE
      QueryTable queryTable = QueryTable.fromQueryCtx(ctx.tableQuery(0));

      // where CONDITION
      if (ctx.K_WHERE() != null) {
        SQLParser.ConditionContext selectCondition = ctx.multipleCondition().condition();
        ArrayList<Row> newRows =
            QueryTable.getRowsSatisfyWhereClause(
                queryTable.iterator(), queryTable.columns, selectCondition);
        queryTable.rows = newRows;
      }

      // select ATTR
      ArrayList<Integer> columnIndexs = new ArrayList<>();
      ArrayList<String> finalColumnNames = new ArrayList<>();
      boolean isSelectAll = false;
      for (SQLParser.ResultColumnContext columnContext : ctx.resultColumn()) {
        if (columnContext.getText().equals("*")) {
          isSelectAll = true;
          break;
        }
        String columnName = columnContext.columnFullName().getText().toLowerCase();
        finalColumnNames.add(columnName);
        int index = QueryTable.getIndexOfAttrName(queryTable.columns, columnName);
        columnIndexs.add(index);
      }
      if (!isSelectAll) queryTable.filteredOnColumns(columnIndexs);

      return queryTable.toQueryResult();

    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  public String showTable(String tableName) {
    try {
      Database database = getAndAssumeCurrentDatabase();
      Table table = database.get(tableName);
      ArrayList<Column> columns = new ArrayList<Column>();
      int columnNum = table.columns.size();
      String output = "table " + tableName + "\n";
      for (int i = 0; i < columnNum; i++) {
        columns.add(table.columns.get(i));
      }
      for (int i = 0; i < columnNum; i++) {
        Column column = columns.get(i);
        if (column.getColumnType().toString().toUpperCase(Locale.ROOT).equals("STRING")) {
          output =
              output
                  + column.getColumnName().toString().toLowerCase(Locale.ROOT)
                  + "("
                  + column.getMaxLength()
                  + ")"
                  + "\t"
                  + column.getColumnType().toString().toUpperCase(Locale.ROOT)
                  + "\t";
        } else {
          output =
              output
                  + column.getColumnName().toString().toLowerCase(Locale.ROOT)
                  + "\t"
                  + column.getColumnType().toString().toUpperCase(Locale.ROOT)
                  + "\t";
        }
        if (columns.get(i).isPrimary()) {
          output = output + "PRIMARY KEY\t";
        }
        if (columns.get(i).cantBeNull()) {
          output = output + "NOT NULL";
        }
        output += "\n";
      }
      return output + "\n";
    } finally {

    }
  }

  public void deleteTable(String name) {
    try {
      Database database = getAndAssumeCurrentDatabase();
      database.dropTable(name);
    } finally {

    }
  }

  private void recover() {

    lock.writeLock().lock();
    try {
      File readDatabasesFile = new File(MANAGER_DATAPATH);
      if (!readDatabasesFile.exists()) return;
      try {
        FileReader fileReader = new FileReader(MANAGER_DATAPATH);
        BufferedReader reader = new BufferedReader(fileReader);
        String line;
        while ((line = reader.readLine()) != null) {
          createDatabaseIfNotExists(line);
        }
        reader.close();
      } catch (Exception e) {
        e.printStackTrace();
        // throw exception
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();

    private ManagerHolder() {}
  }
}

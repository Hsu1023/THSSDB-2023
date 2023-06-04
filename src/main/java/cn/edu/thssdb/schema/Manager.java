package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Global;
import org.antlr.v4.runtime.RuleContext;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class Manager {
  private HashMap<String, Database> databases;

  private Database curDatabase = null;

  private static String MANAGER_DATAPATH = Global.DATA_PATH + File.separator + "manager.db";
  //  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private String seperateLevel =
      "READ_COMMITTED"; // could be changed into "READ_COMMITTED" or "SERIALIZABLE"

  public ArrayList<Long> transaction_list =
      new ArrayList<Long>(); // 事务列表，begin transaction开启，commit结束
  public ArrayList<Long> session_queue = new ArrayList<Long>(); // 由于锁阻塞的session队列
  public HashMap<Long, ArrayList<String>> s_lock_dict =
      new HashMap<Long, ArrayList<String>>(); // 记录每个session取得了哪些表的s锁
  public HashMap<Long, ArrayList<String>> x_lock_dict =
      new HashMap<Long, ArrayList<String>>(); // 记录每个session取得了哪些表的x锁

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
    //    lock.writeLock().lock();
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
      //      lock.writeLock().unlock();
    }
  }

  public void deleteDatabase(String name) {

    Boolean change = false;
    //    lock.writeLock().lock();
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
      //      lock.writeLock().unlock();
    }
  }

  public ArrayList<String> getDatabaseNames() {
    return new ArrayList<>(databases.keySet());
  }

  public void switchDatabase(String name) {
    try {
      //      lock.readLock().lock();
      if (!databases.containsKey(name)) {
        System.out.println("[DEBUG] " + "non-existed db " + name);
        throw new DatabaseNotExistException();
      }
      curDatabase = databases.get(name);
    } finally {
      //      lock.readLock().unlock();
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
    //    try {
    //      for (String i : databases.keySet()) {
    //        Database database = databases.get(i);
    //        for (String j : database.tables.keySet()) {
    //          Table table = database.tables.get(j);
    //          table.persist();
    //        }
    //      }
    //    } catch (Exception e) {
    //      e.printStackTrace();
    //    }
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
          // change length
          if (type != ColumnType.STRING)
            length = (type == ColumnType.INT || type == ColumnType.FLOAT) ? 2 : 4;
          columnItems.add(new Column(columnName, type, 0, notNull, length)); // 新增column Item
        }
        // table constraint
        else if (SQLParser.TableConstraintContext.class.isInstance(ctx.getChild(i))) {
          SQLParser.TableConstraintContext tableConstraintContext =
              (SQLParser.TableConstraintContext) ctx.getChild(i);
          // primary key constraint
          if (tableConstraintContext.K_PRIMARY() != null) {
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

            int columnNum = columnItems.size();
            for (int j = 0; j < columnNum; j++) {
              if (primaryKeys.contains(columnItems.get(j).getName())) {
                columnItems.get(j).setPrimary(1);
              }
            }
          }
          // foreign key constraint
          else if (tableConstraintContext.K_FOREIGN() != null) {
            List<String> localColumnList = tableConstraintContext.columnName0().stream()
                    .map(RuleContext::toString)
                    .collect(Collectors.toList());
            List<String> foreignColumnList = tableConstraintContext.columnName1().stream()
                    .map(RuleContext::toString)
                    .collect(Collectors.toList());
            if (localColumnList.size() != foreignColumnList.size())
              throw new SchemaLengthMismatchException(
                      foreignColumnList.size(),
                      localColumnList.size(),
                      "wrong create table: foreign key constraint length mismatch"
              );

            String foreignTableName = tableConstraintContext.tableName().toString();
            if (!curDatabase.tables.containsKey(foreignTableName)) {
              throw new TableNotExistException();
            }
            Table foreignTable = database.get(foreignTableName);
            List<String> foreignTablePrimaryKeyList
                    = foreignTable.columns.stream()
                    .filter(Column::isPrimary)
                    .map(Column::getColumnName)
                    .collect(Collectors.toList());

            if (!new HashSet<String>(foreignColumnList).equals(new HashSet<String>(foreignTablePrimaryKeyList)))
              throw new NoPrimaryKeyException();




          } else {

          }
        }
        // failed to parse item
        else {
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

  public void insert(SQLParser.InsertStmtContext ctx, long session) {
    try {
      // get table
      Database database = getAndAssumeCurrentDatabase();
      String tableName = ctx.tableName().children.get(0).toString();
      if (!curDatabase.tables.containsKey(tableName)) {
        throw new TableNotExistException();
      }
      Table table = database.get(tableName);

      if (seperateLevel == "SERIALIZABLE") {
        // lock
        while (true) {
          if (!session_queue.contains(session)) { // 不在原来的阻塞队列里
            int get_lock = table.get_x_lock(session);
            if (get_lock != -1) {
              if (get_lock == 1) {
                ArrayList<String> tmp = x_lock_dict.get(session);
                if (tmp == null) {
                  ArrayList<String> tmp1 = new ArrayList<String>();
                  tmp1.add(tableName);
                  x_lock_dict.put(session, tmp1);
                } else {
                  tmp.add(tableName);
                  x_lock_dict.put(session, tmp);
                }
              }
              break;
            } else {
              session_queue.add(session);
            }
          } else { // 之前等待的session
            if (session_queue.get(0) == session) { // 在原来的阻塞队列里
              int get_lock = table.get_x_lock(session);
              if (get_lock != -1) {
                if (get_lock == 1) {
                  ArrayList<String> tmp = x_lock_dict.get(session);
                  if (tmp == null) {
                    ArrayList<String> tmp1 = new ArrayList<String>();
                    tmp1.add(tableName);
                    x_lock_dict.put(session, tmp1);
                  } else {
                    tmp.add(tableName);
                    x_lock_dict.put(session, tmp);
                  }
                }
                session_queue.remove(0);
                break;
              }
            }
          }
        }
      }
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
      if (seperateLevel == "SERIALIZABLE") {
        // 判断是否默认commit
        if (!transaction_list.contains(
            session)) { // 如果没有begin transaction的情况，即当前会话不在transaction_list中
          // 释放锁
          Database the_database = curDatabase;
          ArrayList<String> table_list = x_lock_dict.get(session);
          for (String table_name : table_list) {
            Table the_table = the_database.get(table_name);
            the_table.free_x_lock(session);
          }
          table_list.clear();
          x_lock_dict.put(session, table_list);
        }
      }

    } finally {

    }
  }

  public void delete(SQLParser.DeleteStmtContext ctx, long session) {
    try {
      // get table
      Database database = getAndAssumeCurrentDatabase();
      String tableName = ctx.tableName().children.get(0).toString();
      if (!database.tables.containsKey(tableName)) {
        throw new TableNotExistException();
      }
      Table table = database.get(tableName);

      if (seperateLevel == "SERIALIZABLE") {
        // lock
        while (true) {
          if (!session_queue.contains(session)) { // 不在原来的阻塞队列里
            int get_lock = table.get_x_lock(session);
            if (get_lock != -1) {
              if (get_lock == 1) {
                ArrayList<String> tmp = x_lock_dict.get(session);
                if (tmp == null) {
                  ArrayList<String> tmp1 = new ArrayList<String>();
                  tmp1.add(tableName);
                  x_lock_dict.put(session, tmp1);
                } else {
                  tmp.add(tableName);
                  x_lock_dict.put(session, tmp);
                }
              }
              break;
            } else {
              session_queue.add(session);
            }
          } else { // 之前等待的session
            if (session_queue.get(0) == session) { // 在原来的阻塞队列里
              int get_lock = table.get_x_lock(session);
              if (get_lock != -1) {
                if (get_lock == 1) {
                  ArrayList<String> tmp = x_lock_dict.get(session);
                  if (tmp == null) {
                    ArrayList<String> tmp1 = new ArrayList<String>();
                    tmp1.add(tableName);
                    x_lock_dict.put(session, tmp1);
                  } else {
                    tmp.add(tableName);
                    x_lock_dict.put(session, tmp);
                  }
                }
                session_queue.remove(0);
                break;
              }
            }
          }
        }
      }

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
          if (curEntry.value == null) continue;
          if ((comparator.EQ() != null && curEntry.compareTo(comparedEntry) == 0)
              || (comparator.NE() != null && curEntry.compareTo(comparedEntry) != 0)
              || (comparator.GE() != null && curEntry.compareTo(comparedEntry) >= 0)
              || (comparator.LE() != null && curEntry.compareTo(comparedEntry) <= 0)
              || (comparator.GT() != null && curEntry.compareTo(comparedEntry) > 0)
              || (comparator.LT() != null && curEntry.compareTo(comparedEntry) < 0))
            table.delete(curRow);
        }
      }

      System.out.println("[DEBUG]" + "current number of rows is " + table.getRowSize());

      if (seperateLevel == "SERIALIZABLE") {
        // 判断是否默认commit
        if (!transaction_list.contains(
            session)) { // 如果没有begin transaction的情况，即当前会话不在transaction_list中
          // 释放锁
          Database the_database = curDatabase;
          ArrayList<String> table_list = x_lock_dict.get(session);
          for (String table_name : table_list) {
            Table the_table = the_database.get(table_name);
            the_table.free_x_lock(session);
          }
          table_list.clear();
          x_lock_dict.put(session, table_list);
        }
      }

    } finally {

    }
  }

  public void update(SQLParser.UpdateStmtContext ctx, long session) {
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
      // lock
      while (true) {
        if (!session_queue.contains(session)) { // 不在原来的阻塞队列里
          int get_lock = table.get_x_lock(session);
          if (get_lock != -1) {
            if (get_lock == 1) {
              ArrayList<String> tmp = x_lock_dict.get(session);
              if (tmp == null) {
                ArrayList<String> tmp1 = new ArrayList<String>();
                tmp1.add(tableName);
                x_lock_dict.put(session, tmp1);
              } else {
                tmp.add(tableName);
                x_lock_dict.put(session, tmp);
              }
            }
            break;
          } else {
            session_queue.add(session);
          }
        } else { // 之前等待的session
          if (session_queue.get(0) == session) { // 在原来的阻塞队列里
            int get_lock = table.get_x_lock(session);
            if (get_lock != -1) {
              System.out.println("get x lock");
              if (get_lock == 1) {
                ArrayList<String> tmp = x_lock_dict.get(session);
                if (tmp == null) {
                  ArrayList<String> tmp1 = new ArrayList<String>();
                  tmp1.add(tableName);
                  x_lock_dict.put(session, tmp1);
                } else {
                  tmp.add(tableName);
                  x_lock_dict.put(session, tmp);
                }
              }
              session_queue.remove(0);
              break;
            }
          }
        }
      }

      // update
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
        for (int i = 0; i < columns.size(); i++) {
          if (columns.get(i).getName().equals(attr)) {
            columnIndex = i;
            curColumn = columns.get(i);
            break;
          }
        }
        if (columnIndex == -1) {
          throw new AttributeNotExistException();
        }
        Entry comparedEntry = curColumn.parseEntry(value);
        while (rowIterator.hasNext()) {
          Row curRow = rowIterator.next();
          Entry curEntry = curRow.getEntries().get(columnIndex);
          if (curEntry.value == null) continue;
          if ((comparator.EQ() != null && curEntry.compareTo(comparedEntry) == 0)
              || (comparator.NE() != null && curEntry.compareTo(comparedEntry) != 0)
              || (comparator.GE() != null && curEntry.compareTo(comparedEntry) >= 0)
              || (comparator.LE() != null && curEntry.compareTo(comparedEntry) <= 0)
              || (comparator.GT() != null && curEntry.compareTo(comparedEntry) > 0)
              || (comparator.LT() != null && curEntry.compareTo(comparedEntry) < 0)) {
            ArrayList<Entry> oldRowEntries = new ArrayList<>(curRow.getEntries());
            oldRowEntries.set(updateIndex, attrValue);
            Row newRow = new Row(oldRowEntries);
            table.update(curRow, newRow);
          }
        }
      }
      System.out.println("[DEBUG]" + "current number of rows is " + table.getRowSize());
      // 判断是否默认commit
      if (!transaction_list.contains(
          session)) { // 如果没有begin transaction的情况，即当前会话不在transaction_list中
        // 释放锁
        Database the_database = curDatabase;
        ArrayList<String> table_list = x_lock_dict.get(session);
        for (String table_name : table_list) {
          Table the_table = the_database.get(table_name);
          the_table.free_x_lock(session);
        }
        table_list.clear();
        x_lock_dict.put(session, table_list);
      }
    } finally {
    }
  }

  QueryTable getFinalQueryTable(SQLParser.TableQueryContext query) {
    QueryTable left_table = null, right_table = null;
    Database database = Manager.getInstance().curDatabase;
    System.out.println(database.get(query.tableName(0).getText()));
    System.out.println(database.get(query.tableName(1).getText()));
    left_table = new QueryTable(database.get(query.tableName(0).getText()));
    right_table = new QueryTable(database.get(query.tableName(1).getText()));
    SQLParser.ConditionContext joinCondition = null;
    if (query.K_ON() != null) {
      joinCondition = query.multipleCondition().condition();
    }
    QueryTable cross_table = new QueryTable(left_table, right_table, joinCondition);
    return cross_table;
  }

  public QueryResult select(SQLParser.SelectStmtContext ctx, long session) {
    try {
      // 获取select语句包括的table names
      ArrayList<String> table_names = new ArrayList<>();
      for (SQLParser.TableNameContext subCtx : ctx.tableQuery(0).tableName()) {
        System.out.println("table name: " + subCtx.getText().toLowerCase());
        table_names.add(subCtx.getText().toLowerCase());
      }

      // lock
      while (true) {
        if (!session_queue.contains(session)) // 不在之前的阻塞队列里
        {
          ArrayList<Integer> lock_result = new ArrayList<>();
          for (String name : table_names) {
            Table the_table = curDatabase.get(name);
            int get_lock = the_table.get_s_lock(session);
            lock_result.add(get_lock);
          }
          if (lock_result.contains(-1)) // 有没拿到s锁的
          {
            for (String table_name : table_names) {
              Table the_table = curDatabase.get(table_name);
              the_table.free_s_lock(session);
            }
            session_queue.add(session);
          } else {
            // 拿到了s锁，在manager里的s_lock_list里加
            ArrayList<String> tmp = s_lock_dict.get(session);
            if (tmp == null) {
              ArrayList<String> tmp1 = new ArrayList<String>();
              for (String table_name : table_names) {
                tmp1.add(table_name);
              }
              s_lock_dict.put(session, tmp1);
            } else {
              for (String table_name : table_names) {
                tmp.add(table_name);
              }
              s_lock_dict.put(session, tmp);
            }
            break;
          }
        } else // 之前等待的session
        {
          if (session_queue.get(0) == session) // 只查看阻塞队列开头session
          {
            ArrayList<Integer> lock_result = new ArrayList<>();
            for (String name : table_names) {
              Table the_table = curDatabase.get(name);
              int get_lock = the_table.get_s_lock(session);
              lock_result.add(get_lock);
            }
            if (!lock_result.contains(-1)) {
              session_queue.remove(0);
              break;
            } else {
              for (String table_name : table_names) {
                Table the_table = curDatabase.get(table_name);
                the_table.free_s_lock(session);
              }
              throw new RuntimeException("Read uncommitted data!");
            }
          }
        }
      }

      QueryTable finalTable = null;
      // from TABLE
      SQLParser.TableQueryContext query = ctx.tableQuery().get(0); // 只有1个query
      if (query.getChildCount() == 1) {
        finalTable = QueryTable.fromQueryCtx(ctx.tableQuery(0));
      }
      // join
      else {
        finalTable = getFinalQueryTable(query);
      }

      // where CONDITION
      if (ctx.K_WHERE() != null) {
        SQLParser.ConditionContext selectCondition = ctx.multipleCondition().condition();
        ArrayList<Row> newRows =
            QueryTable.getRowsSatisfyWhereClause(
                finalTable.iterator(), finalTable.columns, selectCondition);
        finalTable.rows = newRows;
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
        int index = QueryTable.getIndexOfAttrName(finalTable.columns, columnName);
        columnIndexs.add(index);
      }
      if (!isSelectAll) finalTable.filteredOnColumns(columnIndexs);

      if (seperateLevel == "READ_COMMITTED") {
        // free s lock
        for (String table_name : table_names) {
          Table the_table = curDatabase.get(table_name);
          the_table.free_s_lock(session);
        }
      } else if (seperateLevel == "SERIALIZABLE") {
        if (!transaction_list.contains(session)) { // 单句
          // free s lock
          for (String table_name : table_names) {
            Table the_table = curDatabase.get(table_name);
            the_table.free_s_lock(session);
          }
        }
      }
      return finalTable.toQueryResult();
    } finally {
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

    //    lock.writeLock().lock();
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
      //      lock.writeLock().unlock();
    }
  }

  /*
  开始transaction
  */
  public void beginTransaction(SQLParser.BeginTransactionStmtContext ctx, long session) {
    try {
      // 如果是新开启的一个事务，要新创建s,x锁记录表
      if (transaction_list == null || !transaction_list.contains(session)) {
        transaction_list.add(session);
        System.out.println(transaction_list);
        ArrayList<String> s_lock_tables = new ArrayList<>();
        ArrayList<String> x_lock_tables = new ArrayList<>();
        s_lock_dict.put(session, s_lock_tables);
        x_lock_dict.put(session, x_lock_tables);
      } else {
        System.out.println("not a new session");
      }

    } finally {

    }
  }

  /*
  commit
   */
  public void commit(SQLParser.CommitStmtContext ctx, long session) {
    try {
      if (transaction_list.contains(session)) {
        Database the_database = curDatabase;
        transaction_list.remove(session);
        // free x lock
        ArrayList<String> x_table_list = x_lock_dict.get(session);
        for (String table_name : x_table_list) {
          Table the_table = the_database.get(table_name);
          the_table.free_x_lock(session);
        }
        x_table_list.clear();
        x_lock_dict.put(session, x_table_list);

        // if SERIALIZABLE free s lock
        if (seperateLevel == "SERIALIZABLE") {
          System.out.println("free s lock");
          ArrayList<String> s_table_list =
              s_lock_dict.get(session); // 虽然更新了Table里的s_lock_list，但是没更新manager里的s_lock_dict
          System.out.println(s_table_list);
          for (String table_name : s_table_list) {
            Table the_table = the_database.get(table_name);
            the_table.free_s_lock(session);
          }
          s_table_list.clear();
          s_lock_dict.put(session, s_table_list);
        }
      } else {
        System.out.println("session not in the list");
      }
    } finally {

    }
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();

    private ManagerHolder() {}
  }
}

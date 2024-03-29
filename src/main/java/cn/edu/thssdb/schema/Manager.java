package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.index.PageManager;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.Logger;
import cn.edu.thssdb.utils.PathUtil;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

// import cn.edu.thssdb.index.PageManager;

public class Manager {
  private HashMap<String, Database> databases;

  private Database curDatabase = null;

  private static String MANAGER_DATAPATH = Global.DATA_PATH + File.separator + "manager.db";
  //  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private String seperateLevel = Global.SEPERATE_LEVEL;
  public Lock meta_lock = new ReentrantLock();

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

  public static Logger logger = new Logger(Global.LOG_PATH, "DATABASE");

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

  public void createDatabaseIfNotExists(String name) {
    // TODO
    Boolean change = false;
    //    lock.writeLock().lock();
    try {
      if (!databases.containsKey(name)) {
        Database newDatabase = new Database(name);
        databases.put(name, newDatabase);
        change = true;
        //        System.out.println("[DEBUG] " + "create db " + name);
      } else {
        //        System.out.println("[DEBUG] " + "duplicated db " + name);
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
        PageManager.deleteDBBuffer(name);
        logger.dropDatabase(name);
        Database db = databases.get(name);
        Table[] tables = db.tables.values().toArray(new Table[0]);
        for (int i = 0; i < tables.length; i++) {
          db.dropTable(tables[i].tableName);
        }
        File file = new File(PathUtil.getDBFolderPath(name));
        if (file.exists()) {
          file.delete();
        }
        databases.remove(name);
        change = true;
        //        System.out.println("[DEBUG] " + "delete db " + name);

      } else {
        //        System.out.println("[DEBUG] " + "non-existed db " + name);
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

  public void checkPoint() {
    logger.checkPoint();
    PageManager.checkPoint();
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
      List<ForeignKeyConstraint> foreignKeyConstraintList = new ArrayList<>();
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
                  .toString();
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
                      .toString();
              primaryKeys.add(columnName);
            }

            int columnNum = columnItems.size();
            for (int j = 0; j < columnNum; j++) {
              if (primaryKeys.contains(columnItems.get(j).getName())) {
                columnItems.get(j).setPrimary(1);
              }
            }
            ;
          }
          // foreign key constraint
          else if (tableConstraintContext.K_FOREIGN() != null) {
            String localColumnName =
                tableConstraintContext.columnName(0).children.get(0).toString();
            String foreignColumnName =
                tableConstraintContext.columnName(1).children.get(0).toString();
            String foreignTableName = tableConstraintContext.tableName().children.get(0).toString();

            //            System.out.println("[DEBUG] " + localColumnName);
            //            System.out.println("[DEBUG] " + foreignColumnName);
            //            System.out.println("[DEBUG] " + foreignTableName);

            if (!curDatabase.tables.containsKey(foreignTableName)) {
              throw new TableNotExistException();
            }
            Table foreignTable = database.get(foreignTableName);
            Column foreignTablePrimaryColumn =
                foreignTable.columns.stream()
                    .filter(column -> column.isPrimary())
                    .collect(Collectors.toList())
                    .get(0);

            if (!Objects.equals(foreignTablePrimaryColumn.getColumnName(), foreignColumnName))
              throw new NoPrimaryKeyException();

            Column localColumn =
                columnItems.stream()
                    .filter(column -> Objects.equals(column.getColumnName(), localColumnName))
                    .collect(Collectors.toList())
                    .get(0);

            foreignKeyConstraintList.add(
                new ForeignKeyConstraint(foreignTable, localColumn, foreignTablePrimaryColumn));
          } else {

          }
        }
        // failed to parse item
        else {
        }
      }
      database.create(
          tableName, columnItems.toArray(new Column[columnItems.size()]), foreignKeyConstraintList);
    } finally {

    }
  }

  public Database getAndAssumeCurrentDatabase() {
    if (curDatabase == null) {
      System.out.println("[DEBUG] " + "current db is null");
      // throw new DatabaseNotExistException();
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

      if (seperateLevel.equals("SERIALIZABLE")) {
        // lock
        while (true) {
          synchronized (meta_lock) {
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
      }
      // get value list
      List<SQLParser.ValueEntryContext> values = ctx.valueEntry();
      ArrayList<String> valueStringList = new ArrayList<>();
      for (SQLParser.ValueEntryContext value : values) {
        for (int i = 0; i < value.literalValue().size(); i++) {
          String valueString = value.literalValue(i).getText();
          valueStringList.add(valueString);
        }
        // only first row
        break;
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
      //      System.out.println(allColumns);
      //      System.out.println(selectedColumns);
      for (Column column : allColumns) {
        int id = selectedColumns.indexOf(column);
        //        System.out.println(id);
        String valueString = "NULL";
        if (id != -1) valueString = valueStringList.get(id);
        Entry entry = column.parseEntry(valueString);
        entries.add(entry);
      }
      Row newRow = new Row(entries);
      try {
        logger.insert(database.getName(), table.tableName, newRow);
        table.insert(newRow);
      } catch (Exception e) {
        e.printStackTrace();
      }
      //      System.out.println("[DEBUG]" + "current number of rows is " + table.getRowSize());

      // for multiple values
      for (SQLParser.ValueEntryContext value : values.subList(1, values.size())) {
        valueStringList = new ArrayList<>();
        for (int i = 0; i < value.literalValue().size(); i++) {
          String valueString = value.literalValue(i).getText();
          valueStringList.add(valueString);
        }
        if (valueStringList.size() != selectedColumns.size()) {
          throw new SchemaLengthMismatchException(
              selectedColumns.size(),
              valueStringList.size(),
              "wrong insert operation (columns unmatched)!");
        }

        entries = new ArrayList<>();
        for (Column column : allColumns) {
          int id = selectedColumns.indexOf(column);
          String valueString = "NULL";
          if (id != -1) valueString = valueStringList.get(id);
          Entry entry = column.parseEntry(valueString);
          entries.add(entry);
        }
        try {
          logger.insert(database.getName(), table.tableName, newRow);
          newRow = new Row(entries);
          table.insert(newRow);
        } catch (Exception e) {
          e.printStackTrace();
        }
        //        System.out.println("[DEBUG]" + "current number of rows is " + table.getRowSize());
      }

    } finally {
      synchronized (meta_lock) {
        if (seperateLevel.equals("SERIALIZABLE")) {
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
      }
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

      if (seperateLevel.equals("SERIALIZABLE")) {
        // lock
        while (true) {
          synchronized (meta_lock) {
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
      }

      ArrayList<Column> columns = table.columns;

      Iterator<Row> rowIterator = table.iterator();

      if (ctx.K_WHERE() == null) {
        while (rowIterator.hasNext()) {
          Row curRow = rowIterator.next();
          logger.delete(database.getName(), table.tableName, curRow);
          table.delete(curRow);
        }
      } else {
        SQLParser.ConditionContext condition = ctx.multipleCondition().condition();
        SQLParser.ComparerContext attrComparer = condition.expression(0).comparer();
        String attr = attrComparer.columnFullName().columnName().getText();
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
              || (comparator.LT() != null && curEntry.compareTo(comparedEntry) < 0)) {
            //            logger.session("SESSION:" + session + " DELETE " + database.getName() + "
            // " + tableName + " " + curRow.toString());
            //            logger.message("delete:" + session + ":" + database.getName() + ":" +
            // table.tableName);
            try {
              logger.delete(database.getName(), table.tableName, curRow);
              table.delete(curRow);
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      }

      //      System.out.println("[DEBUG]" + "current number of rows is " + table.getRowSize());

    } finally {
      synchronized (meta_lock) {
        if (seperateLevel.equals("SERIALIZABLE")) {
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
      }
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
      // lock
      while (true) {
        synchronized (meta_lock) {
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
                //                System.out.println("get x lock");
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

      String columnName = ctx.columnName().getText();
      int updateIndex = table.getColumnIndexByName(columnName);
      Column selectedColumn = columns.get(updateIndex);
      Entry attrValue =
          selectedColumn.parseEntry(ctx.expression().comparer().literalValue().getText());

      // update
      if (ctx.K_WHERE() == null) {
        while (rowIterator.hasNext()) {
          Row curRow = rowIterator.next();
          ArrayList<Entry> oldRowEntries = new ArrayList<>(curRow.getEntries());
          oldRowEntries.set(updateIndex, attrValue);
          Row newRow = new Row(oldRowEntries);
          logger.update(database.getName(), table.tableName, curRow, newRow);
          table.update(curRow, newRow);
        }
      } else {
        SQLParser.ConditionContext condition = ctx.multipleCondition().condition();
        SQLParser.ComparerContext attrComparer = condition.expression(0).comparer();
        String attr = attrComparer.columnFullName().columnName().getText();
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
            try {
              //            logger.message("UPDATE:" + session + " " + database.getName() + " " +
              // tableName);
              logger.update(database.getName(), table.tableName, curRow, newRow);
              table.update(curRow, newRow);
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      }
      //      System.out.println("[DEBUG]" + "current number of rows is " + table.getRowSize());
      // 判断是否默认commit

    } finally {
      synchronized (meta_lock) {
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
    }
  }

  QueryTable getFinalQueryTable(SQLParser.TableQueryContext query) {
    Database database = Manager.getInstance().curDatabase;
    List<SQLParser.TableNameContext> table_list = query.tableName();

    SQLParser.ConditionContext joinCondition = null;
    if (query.K_ON() != null) {
      joinCondition = query.multipleCondition().condition();
    }

    // 先找到条件左右的table和column
    String leftColumnName = null, rightColumnName = null;
    leftColumnName = joinCondition.expression(0).getText();
    //    System.out.println(leftColumnName);
    rightColumnName = joinCondition.expression(1).getText();
    //    System.out.println(rightColumnName);
    QueryTable left_table = null, right_table = null;
    // 找左侧table
    for (SQLParser.TableNameContext table : table_list) {
      QueryTable tmp = new QueryTable(database.get(table.getText()));
      int leftColumnIndex = QueryTable.getIndexOfAttrName(tmp.columns, leftColumnName);
      if (leftColumnIndex != -1) {
        left_table = tmp;
        table_list.remove(table);
        break;
      }
    }
    // 找右侧table
    for (SQLParser.TableNameContext table : table_list) {
      QueryTable tmp = new QueryTable(database.get(table.getText()));
      int rightColumnIndex = QueryTable.getIndexOfAttrName(tmp.columns, rightColumnName);
      if (rightColumnIndex != -1) {
        right_table = tmp;
        table_list.remove(table);
        break;
      }
    }
    QueryTable cross_table = new QueryTable(left_table, right_table, joinCondition);
    // 如果只有两张表，cross_table就是最终结果了
    if (table_list.size() == 0) {
      return cross_table;
    }
    // 否则，要返回cross_table和其它表的笛卡尔积
    else {
      for (SQLParser.TableNameContext table : table_list) {
        cross_table = new QueryTable(cross_table, new QueryTable(database.get(table.getText())));
      }
      return cross_table;
    }
  }

  public QueryResult select(SQLParser.SelectStmtContext ctx, long session) {
    ArrayList<String> table_names = new ArrayList<>();
    QueryTable finalTable = null;
    try {
      // 获取select语句包括的table names
      for (SQLParser.TableNameContext subCtx : ctx.tableQuery(0).tableName()) {
        //        System.out.println("table name: " + subCtx.getText());
        table_names.add(subCtx.getText());
      }
      // lock
      while (true) {
        synchronized (meta_lock) {
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

              // 拿到了s锁，在manager里的s_lock_list里加
              if (!lock_result.contains(-1)) {
                // 更新 s_lock_dict
                ArrayList<String> tmp1 = new ArrayList<String>();
                for (String table_name : table_names) {
                  tmp1.add(table_name);
                }
                s_lock_dict.put(session, tmp1);

                session_queue.remove(0);
                break;
              } else {
                for (String table_name : table_names) {
                  Table the_table = curDatabase.get(table_name);
                  the_table.free_s_lock(session);
                }
                //                throw new RuntimeException("Read uncommitted data!");
              }
            }
          }
        }
      }

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
        String columnName = columnContext.columnFullName().getText();
        finalColumnNames.add(columnName);
        int index = QueryTable.getIndexOfAttrName(finalTable.columns, columnName);
        columnIndexs.add(index);
      }
      if (!isSelectAll) finalTable.filteredOnColumns(columnIndexs);
      return finalTable.toQueryResult();
    } catch (Exception e) {
      //      System.out.println("Select Catch");
      throw e;
    } finally {
      //      System.out.println("Select Finally");
      synchronized (meta_lock) {
        if (seperateLevel.equals("READ_COMMITTED")) {
          for (String table_name : table_names) {
            Table the_table = curDatabase.get(table_name);
            the_table.free_s_lock(session);
          }
        } else if (seperateLevel.equals("SERIALIZABLE")) {
          if (!transaction_list.contains(
              session)) { // 如果没有begin transaction的情况，即当前会话不在transaction_list中
            //            System.out.println("Release Select S locks");
            // 释放锁
            Database the_database = curDatabase;
            for (String table_name : table_names) {
              Table the_table = the_database.get(table_name);
              the_table.free_s_lock(session);
            }

            ArrayList<String> table_list = s_lock_dict.get(session);
            table_list.clear();
            s_lock_dict.put(session, table_list);
          }
        }
      }
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
                  + column.getColumnName().toString()
                  + "("
                  + column.getMaxLength()
                  + ")"
                  + "\t"
                  + column.getColumnType().toString().toUpperCase(Locale.ROOT)
                  + "\t";
        } else {
          output =
              output
                  + column.getColumnName().toString()
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
      logger.dropTable(curDatabase.getName(), name);
      database.dropTable(name);
    } finally {

    }
  }

  private void recover() {
    if (!Global.RECOVER_FROM_DISC)
      //    lock.writeLock().lock();
      try {
        File readDatabasesFile = new File(MANAGER_DATAPATH);
        if (!readDatabasesFile.exists()) return;
        try {
          FileReader fileReader = new FileReader(MANAGER_DATAPATH);
          BufferedReader reader = new BufferedReader(fileReader);
          String line;
          while ((line = reader.readLine()) != null) {
            createDatabaseIfNotExists(line); // recover table inside init databse
          }
          reader.close();

          List<String> lines = logger.readLog();
          for (int i = 0; i < lines.size(); i++) {
            line = lines.get(i);
            String[] log = line.split("@", 4);
            Database tmpDatabase = databases.get(log[1]);
            Table tmpTable = tmpDatabase.get(log[2]);
            if (log[0].equals("DELETE")) tmpTable.delete(log[3]);
            else if (log[0].equals("INSERT")) tmpTable.insert(log[3]);
          }

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
  public void beginTransaction(long session) {
    //    System.out.println("beginTransaction");
    try {
      synchronized (meta_lock) {
        // 如果是新开启的一个事务，要新创建s,x锁记录表
        if (transaction_list == null || !transaction_list.contains(session)) {
          transaction_list.add(session);
          //          System.out.println(transaction_list);
          ArrayList<String> s_lock_tables = new ArrayList<>();
          ArrayList<String> x_lock_tables = new ArrayList<>();
          s_lock_dict.put(session, s_lock_tables);
          x_lock_dict.put(session, x_lock_tables);
        } else {
          //          System.out.println("not a new session");
        }
      }
    } finally {
    }
  }

  /*
  commit
   */
  public void commit(long session) {
    //    System.out.println("commit");
    try {
      synchronized (meta_lock) {
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
          if (seperateLevel.equals("SERIALIZABLE")) {
            //            System.out.println("commit: free s lock");
            ArrayList<String> s_table_list =
                s_lock_dict.get(session); // 虽然更新了Table里的s_lock_list，但是没更新manager里的s_lock_dict
            //            System.out.println(s_table_list);
            for (String table_name : s_table_list) {
              Table the_table = the_database.get(table_name);
              the_table.free_s_lock(session);
            }
            s_table_list.clear();
            s_lock_dict.put(session, s_table_list);
          }
        } else {
          //          System.out.println("session not in the list");
        }
      }
    } finally {

    }
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();

    private ManagerHolder() {}
  }
}

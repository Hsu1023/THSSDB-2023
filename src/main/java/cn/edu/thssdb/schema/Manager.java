package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.exception.DuplicateKeyException;
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
        throw new DuplicateKeyException();
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

  public void createTableIfNotExist(SQLParser.CreateTableStmtContext ctx) {
    try {
      if (curDatabase == null) {
        System.out.println("[DEBUG] " + "current db is null");
        throw new DatabaseNotExistException();
      } else {
        String tableName =
            ctx.tableName().children.get(0).toString(); // create table tableName tableName
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
                ((SQLParser.ColumnDefContext) ctx.getChild(i))
                    .typeName()
                    .children
                    .get(0)
                    .toString();
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
        }if (curDatabase == null) {
          throw new DatabaseNotExistException();
        } else {
          curDatabase.create(tableName, columnItems.toArray(new Column[columnItems.size()]));
        }
      }
    } finally {

    }
  }

  public String showTable(String tableName) {
    try {
      if (curDatabase == null) {
        System.out.println("[DEBUG] " + "current db is null");
        throw new DatabaseNotExistException();
      } else {
        Table table = curDatabase.get(tableName);
        ArrayList<Column> columns = new ArrayList<Column>();
        int columnNum = table.columns.size();
        String output = "table " + tableName + "\n";
        for (int i = 0; i < columnNum; i++) {
          columns.add(table.columns.get(i));
        }
        for (int i = 0; i < columnNum; i++) {
          Column column = columns.get(i);
          output =
              output
                  + column.getColumnName().toString().toLowerCase(Locale.ROOT)
                  + "("
                  + column.getMaxLength()
                  + ")"
                  + "\t"
                  + column.getColumnType().toString().toUpperCase(Locale.ROOT)
                  + "\t";
          if (columns.get(i).isPrimary()) {
            output = output + "PRIMARY KEY\t";
          }
          if (columns.get(i).cantBeNull()) {
            output = output + "NOT NULL";
          }
          output += "\n";
        }
        return output + "\n";
      }
    } finally {

    }
  }

  public void deleteTable(String name) {
    try {
      if (curDatabase == null) {
        System.out.println("[DEBUG] " + "current db is null");
        throw new DatabaseNotExistException();
      } else {
        curDatabase.dropTable(name);
      }
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

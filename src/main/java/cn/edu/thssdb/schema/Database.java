package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DuplicateTableException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.utils.Global;

import java.awt.*;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {

  private String name;
  public HashMap<String, Table> tables;
  ReentrantReadWriteLock lock;

  public Database(String name) {
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    recover();
  }

  public String getTablesFolderPath() {
    return Global.DATA_PATH + File.separator + this.name + File.separator;
  }

  private void persist() {
    //    String saveFolderPath = getTablesFolderPath();
    try {
      lock.writeLock().lock();
      for (String tableName : tables.keySet()) {
        Table table = tables.get(tableName);
        String savePath = table.getMetaDataPath();
        FileOutputStream fos = new FileOutputStream(savePath);
        OutputStreamWriter writer = new OutputStreamWriter(fos);
        for (Column column : table.columns) writer.write(column.toString() + "\n");
        writer.close();
        fos.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
      // throw exception
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void create(
      String name, Column[] columns, List<ForeignKeyConstraint> foreignKeyConstraintList) {
    try {
      lock.writeLock().lock();
      if (this.tables.containsKey(name)) {
        System.out.println("[DEBUG] " + "duplicated tb " + name);
        throw new DuplicateTableException();
      }
      Table table =
          new Table(this.name, name, columns, this.getTablesFolderPath(), foreignKeyConstraintList);
      this.tables.put(name, table);
      this.persist();
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  public void dropTable(String tableName) {
    try {
      lock.writeLock().lock();
      if (!this.tables.containsKey(tableName)) {
        System.out.println("[DEBUG] " + "non-existed tb " + tableName);
        throw new TableNotExistException();
      }
      Table table = tables.get(tableName);

      String tableMetaPath = table.getMetaDataPath();
      File tableMetaFile = new File(tableMetaPath);
      if (!tableMetaFile.exists() || !tableMetaFile.isFile() || !tableMetaFile.delete()) {
        throw new RuntimeException();
      }

      String tableDataPath = table.getBinPath();
      File tableDataFile = new File(tableDataPath);
      if (tableDataFile.exists() && (!tableDataFile.isFile() || !tableDataFile.delete())) {
        throw new RuntimeException();
      }

      String tableFolerPath = table.getFolderPath();
      File tableFolderPath = new File(tableFolerPath);
      if (!tableFolderPath.exists()
          || !tableFolderPath.isDirectory()
          || !tableFolderPath.delete()) {
        throw new RuntimeException();
      }
      this.tables.remove(tableName);

    } finally {
      lock.writeLock().unlock();
    }
  }

  public String select(QueryTable[] queryTables) {
    // TODO
    QueryResult queryResult = new QueryResult(queryTables);
    return null;
  }

  private void recover() {
    lock.writeLock().lock();
    try {
      File tableFolderFile = new File(getTablesFolderPath());
      File[] tablesFile = tableFolderFile.listFiles();
      if (tablesFile == null) return;
      for (File tableFile : tablesFile) { // db内部folder
        File[] tableFiles = tableFile.listFiles();
        for (File file : tableFiles) { // table内部folder
          if (file.getName().equals("_meta")) {
            String tableName = tableFile.getName();
            FileReader fileReader = new FileReader(file);
            BufferedReader reader = new BufferedReader(fileReader);
            ArrayList<Column> columnList = new ArrayList<>();
            String readLine;
            while ((readLine = reader.readLine()) != null)
              columnList.add(Column.toColumn(readLine));
            Table table =
                new Table(
                    name,
                    tableName,
                    columnList.toArray(new Column[columnList.size()]),
                    getTablesFolderPath(),
                    new ArrayList<>()); // TODO: recover foreign key constraint
            tables.put(tableName, table);

            System.out.println("[DEBUG] " + "recover " + tableName);
            reader.close();
            break;
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void quit() {
    // TODO
  }

  public Table get(String tableName) {
    try {
      lock.readLock().lock();
      if (!this.tables.containsKey(tableName)) {
        System.out.println("[DEBUG] " + "non-existed tb " + tableName);
        throw new TableNotExistException();
      }
      return this.tables.get(tableName);
    } finally {
      lock.readLock().unlock();
    }
  }

  public String getName() {
    return name;
  }
}

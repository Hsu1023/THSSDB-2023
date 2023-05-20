package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DuplicateTableException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {

  private String name;
  private HashMap<String, Table> tables;
  ReentrantReadWriteLock lock;

  public Database(String name) {
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    recover();
  }

  private void persist() {
    // 表数据的保持
  }

  public void create(String name, Column[] columns) {
    try {
      lock.writeLock().lock();
      if (this.tables.containsKey(name)) {
        System.out.println("[DEBUG] " + "duplicated tb " + name);
        throw new DuplicateTableException();
      }
      Table table = new Table(this.name, name, columns);
      this.tables.put(name, table);
      this.persist();
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  public void drop(String tableName) {
    try {
      lock.writeLock().lock();
      if (!this.tables.containsKey(tableName)) {
        System.out.println("[DEBUG] " + "non-existed tb " + tableName);
        throw new TableNotExistException();
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
    // TODO
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
}

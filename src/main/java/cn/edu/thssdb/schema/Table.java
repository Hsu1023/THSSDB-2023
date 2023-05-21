package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.MultiPrimaryKeyException;
import cn.edu.thssdb.exception.NoPrimaryKeyException;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.Pair;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table implements Iterable<Row> {
  ReentrantReadWriteLock lock;
  private String databaseName;
  public String tableName;
  public ArrayList<Column> columns;
  public BPlusTree<Entry, Row> index;
  private int primaryIndex;

  private String databaseFolderPath;
  private String tableFolderPath;

  public Table(String databaseName, String tableName, Column[] columns, String databaseFolderPath) {
    this.lock = new ReentrantReadWriteLock();
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = new ArrayList<>(Arrays.asList(columns));
    this.index = new BPlusTree<>();
    this.primaryIndex = -1;
    this.databaseFolderPath = databaseFolderPath;
    this.tableFolderPath = databaseFolderPath + File.separator + tableName + File.separator;
    File folder = new File(tableFolderPath);
    if (!folder.exists()) {
      boolean created = folder.mkdirs();
      if (!created)
        throw new RuntimeException();
    }

    for (int i = 0; i < this.columns.size(); i++) {
      if (this.columns.get(i).isPrimary()) {
        if (this.primaryIndex >= 0) {
          System.out.println("[DEBUG] " + "multi primary keys");
          throw new MultiPrimaryKeyException();
        }
        this.primaryIndex = i;
      }
    }
    if (this.primaryIndex < 0) {
      System.out.println("[DEBUG] " + "no primary key");
      throw new NoPrimaryKeyException();
    }

    // TODO initiate lock status.

    recover();
  }

  private void recover() {
    // TODO
  }

  public void insert() {
    // TODO
  }

  public void delete() {
    // TODO
  }

  public void update() {
    // TODO
  }

  private void serialize() {
    // TODO
  }

  private ArrayList<Row> deserialize() {
    // TODO
    return null;
  }
  public String getFolderPath(){
    return this.tableFolderPath;
  }
  public String getMetaDataPath(){
    return this.tableFolderPath + "_meta";
  }

  private class TableIterator implements Iterator<Row> {
    private Iterator<Pair<Entry, Row>> iterator;

    TableIterator(Table table) {
      this.iterator = table.index.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Row next() {
      return iterator.next().right;
    }
  }



  @Override
  public Iterator<Row> iterator() {
    return new TableIterator(this);
  }
}

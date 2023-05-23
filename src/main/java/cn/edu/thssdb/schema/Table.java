package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.type.ColumnType;
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
      if (!created) throw new RuntimeException();
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

  public int getColumnIndexByName(String name) {
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).getName().equals(name)) {
        return i;
      }
    }
    return -1;
  }

  private void recover() {
    // TODO
  }

  public void insert(Row row) {
    try {
      // TODO lock control
      this.lock.writeLock().lock();
      this.checkRowValidInTable(row);
      if (this.containsRow(row)) throw new DuplicateKeyException();
      this.index.put(row.getEntries().get(this.primaryIndex), row);
    } finally {
      // TODO lock control
      this.lock.writeLock().unlock();
    }
  }

  public Boolean contains(Row row) {
    return index.contains(row.getEntries().get(primaryIndex));
  }

  public void delete(Row row) {
    lock.writeLock().lock();
    try {
      if (!this.contains(row)) {
        throw new RowNotExistException();
      }
      index.remove(row.getEntries().get(primaryIndex));
    } finally {
      lock.writeLock().unlock();
    }
  }

  public int getRowSize() {
    return index.size();
  }

  public void update(Row oldRow, Row newRow) {
    this.lock.writeLock().lock();
    try {
      checkRowValidInTable(newRow);
      Entry keyEntry = oldRow.getEntries().get(primaryIndex);
      if (!keyEntry.equals(newRow.getEntries().get(primaryIndex)) && containsRow(newRow)) {
        throw new DuplicateKeyException();
      }
      index.update(keyEntry, newRow);
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  private void checkRowValidInTable(Row row) {
    if (row.getEntries().size() != this.columns.size())
      throw new SchemaLengthMismatchException(
          this.columns.size(), row.getEntries().size(), "when check Row Valid In table");
    for (int i = 0; i < row.getEntries().size(); i++) {
      String entryValueType = row.getEntries().get(i).getValueType();
      Column column = this.columns.get(i);
      if (entryValueType.equals("NULL")) {
        if (column.cantBeNull()) throw new NullValueException(column.getColumnName());
      } else {
        if (!entryValueType.equals(column.getColumnType().name()))
          throw new ValueFormatInvalidException("(when check row valid in table)");
        Comparable entryValue = row.getEntries().get(i).value;
        if (entryValueType.equals(ColumnType.STRING.name())
            && ((String) entryValue).length() > column.getMaxLength())
          throw new ValueExceedException(
              column.getColumnName(),
              ((String) entryValue).length(),
              column.getMaxLength(),
              "(when check row valid in table)");
      }
    }
  }

  private Boolean containsRow(Row row) {
    return this.index.contains(row.getEntries().get(this.primaryIndex));
  }

  private void serialize() {
    // TODO
  }

  private ArrayList<Row> deserialize() {
    // TODO
    return null;
  }

  public String getFolderPath() {
    return this.tableFolderPath;
  }

  public String getMetaDataPath() {
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

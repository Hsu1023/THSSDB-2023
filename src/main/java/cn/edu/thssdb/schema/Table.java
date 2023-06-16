package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Table implements Iterable<Row> {
  //  ReentrantReadWriteLock lock;
  private String databaseName;
  public String tableName;
  public ArrayList<Column> columns;
  public BPlusTree<Entry, Row> index;
  public List<ForeignKeyConstraint> foreignKeyConstraintList;
  private int primaryIndex;
  private String databaseFolderPath;
  private String tableFolderPath;

  private int tplock = 0; // 0表示既没加s也没加x; 1表示被加s锁; 2表示被加x锁
  public ArrayList<Long> s_lock_list = new ArrayList<Long>(); // 存储这个表的s锁被哪些session持有
  public ArrayList<Long> x_lock_list = new ArrayList<Long>(); // 存储这个表的x锁被哪些session持有

  public Table(
      String databaseName,
      String tableName,
      Column[] columns,
      String databaseFolderPath,
      List<ForeignKeyConstraint> foreignKeyConstraintList) {
    //    this.lock = new ReentrantReadWriteLock();
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = new ArrayList<>(Arrays.asList(columns));
    this.foreignKeyConstraintList = foreignKeyConstraintList;
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
          //          System.out.println("[DEBUG] " + "multi primary keys");
          throw new MultiPrimaryKeyException();
        }
        this.primaryIndex = i;
      }
    }
    if (this.primaryIndex < 0) {
      //      System.out.println("[DEBUG] " + "no primary key");
      throw new NoPrimaryKeyException();
    }

//    this.index = new BPlusTree<>(databaseName, tableName, columns, primaryIndex, false);
        this.index = new BPlusTree<>();
    // TODO initiate lock status.
    //    recover();
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
    //    try {
    //      // TODO lock control
    //      ArrayList<Row> rowsOnDisk = deserialize();
    //      for (Row row : rowsOnDisk) this.index.put(row.getEntries().get(this.primaryIndex), row);
    //    } finally {
    //      // TODO lock control
    //    }
  }

  public void persist() {
    //    serialize();
  }

  public void insert(Row row) {
    try {
      // TODO lock control
      //      this.lock.writeLock().lock();
      this.checkRowValidInTable(row);
      if (this.containsRow(row)) throw new DuplicateKeyException();
      this.index.put(row.getEntries().get(this.primaryIndex), row);

    } finally {
      // TODO lock control
      //      this.lock.writeLock().unlock();
    }
  }

  public void insert(String row) {
    try {
      String[] info = row.split(", ");
      ArrayList<Entry> entries = new ArrayList<>();
      int i = 0;
      for (Column c : columns) {
        entries.add(new Entry(ColumnType.getColumnTypeValue(c.getColumnType(), info[i])));
        i++;
      }
      index.put(entries.get(primaryIndex), new Row(entries));
    } catch (Exception e) {
      throw e;
    }
  }

  public void delete(String row) {
    ColumnType c = columns.get(primaryIndex).getColumnType();
    String[] info = row.split(", ");
    Entry primaryEntry = new Entry(ColumnType.getColumnTypeValue(c, info[primaryIndex]));
    index.remove(primaryEntry);
  }

  public Boolean contains(Row row) {
    return index.contains(row.getEntries().get(primaryIndex));
  }

  public void delete(Row row) {
    //    lock.writeLock().lock();
    try {
      //      if (!this.contains(row)) {
      //        throw new RowNotExistException();
      //      }
      index.remove(row.getEntries().get(primaryIndex));
    } finally {
      //      lock.writeLock().unlock();
    }
  }

  public int getRowSize() {
    return index.size();
  }

  public void update(Row oldRow, Row newRow) {
    //    this.lock.writeLock().lock();
    try {
      checkRowValidInTable(newRow);
      Entry oldKeyEntry = oldRow.getEntries().get(primaryIndex);
      Entry newKeyEntry = newRow.getEntries().get(primaryIndex);
      if (!oldKeyEntry.equals(newKeyEntry) && containsRow(newRow)) {
        throw new DuplicateKeyException();
      }
      //      index.update(keyEntry, newRow);
      index.remove(oldKeyEntry);
      index.put(newKeyEntry, newRow);

    } finally {
      //      this.lock.writeLock().unlock();
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

    for (ForeignKeyConstraint foreignKeyConstraint : foreignKeyConstraintList)
      if (!foreignKeyConstraint.check(this, row)) throw new ConstraintViolatedException();
  }

  private Boolean containsRow(Row row) {
    return this.index.contains(row.getEntries().get(this.primaryIndex));
  }

  //  private void serialize() {
  //    try {
  //      File tableFolder = new File(this.getFolderPath());
  //      if (!tableFolder.exists() ? !tableFolder.mkdirs() : !tableFolder.isDirectory())
  //        throw new RuntimeException();
  //      File tableFile = new File(this.getDataPath());
  //      if (!tableFile.exists() ? !tableFile.createNewFile() : !tableFile.isFile())
  //        throw new RuntimeException();
  //      FileOutputStream fileOutputStream = new FileOutputStream(this.getDataPath());
  //      ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
  //      for (Row row : this) objectOutputStream.writeObject(row);
  //      objectOutputStream.close();
  //      fileOutputStream.close();
  //    } catch (Exception e) {
  //      throw new RuntimeException();
  //    }
  //  }

  //  private ArrayList<Row> deserialize() {
  //    try {
  //      File tableFolder = new File(this.getFolderPath());
  //      if (!tableFolder.exists() ? !tableFolder.mkdirs() : !tableFolder.isDirectory())
  //        throw new RuntimeException();
  //      File tableFile = new File(this.getDataPath());
  //      if (!tableFile.exists()) return new ArrayList<>();
  //      FileInputStream fileInputStream = new FileInputStream(this.getDataPath());
  //      ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
  //      ArrayList<Row> rowsOnDisk = new ArrayList<>();
  //      Object tmpObj;
  //      while (fileInputStream.available() > 0) {
  //        tmpObj = objectInputStream.readObject();
  //        rowsOnDisk.add((Row) tmpObj);
  //      }
  //      objectInputStream.close();
  //      fileInputStream.close();
  //      return rowsOnDisk;
  //    } catch (Exception e) {
  //      throw new RuntimeException();
  //    }
  //  }

  public void free_s_lock(long session) {
    if (s_lock_list.contains(session)) {
      s_lock_list.remove(session);
      if (s_lock_list.size() == 0) {
        tplock = 0;
      } else {
        tplock = 1;
      }
    }
    //    System.out.println("tplock+-");
    //    System.out.println(tplock);
  }

  public int get_s_lock(long session) {
    int value = 0; // 返回-1代表加锁失败  返回0代表成功但未加锁  返回1代表成功加锁
    if (tplock == 2) {
      if (x_lock_list.contains(session)) { // 自身已经有更高级的锁了 用x锁去读，未加锁
        value = 0;
      } else {
        value = -1; // 别的session占用x锁，未加锁
      }
    } else if (tplock == 1) {
      if (s_lock_list.contains(session)) { // 自身已经有s锁了 用s锁去读，未加锁
        value = 0;
      } else {
        s_lock_list.add(session); // 其他session加了s锁 把自己加上
        tplock = 1;
        value = 1;
      }
    } else if (tplock == 0) {
      s_lock_list.add(session); // 未加锁 把自己加上
      tplock = 1;
      value = 1;
      //      System.out.println(s_lock_list);
    }
    return value;

    //    return 1;
  }

  public void free_x_lock(long session) {
    if (x_lock_list.contains(session)) {
      tplock = 0;
      x_lock_list.remove(session);
    }
  }

  public int get_x_lock(long session) {
    int value = 0; // 返回-1代表加锁失败  返回0代表成功但未加锁  返回1代表成功加锁
    //      System.out.println("tplock");
    //      System.out.println(tplock);
    if (tplock == 2) {
      if (x_lock_list.contains(session)) { // 自身已经取得x锁
        value = 0;
      } else {
        value = -1; // 获取x锁失败
      }
    } else if (tplock == 1) {
      if (s_lock_list.contains(session)) { // 自身已经取得s锁
        // 升级为 x_lock
        tplock = 2;
        x_lock_list.add(session);

        value = 0;
      } else {
        value = -1; // 获取x锁失败
      }
      value = -1; // 正在被其他s锁占用
    } else if (tplock == 0) {
      x_lock_list.add(session);
      tplock = 2;
      value = 1;
    }
    return value;
    //    return 1;
  }

  public String getFolderPath() {
    return this.tableFolderPath;
  }

  public String getMetaDataPath() {
    return this.tableFolderPath + "_meta";
  }

  //  public String getDataPath() {
  //    return this.tableFolderPath + "_data";
  //  }

  public String getBinPath() {
    return this.tableFolderPath + tableName + ".bin";
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

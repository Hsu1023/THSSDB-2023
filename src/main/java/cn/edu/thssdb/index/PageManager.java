package cn.edu.thssdb.index;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PageManager {
  //  String path;

  String databaseName;
  String tableName;
  int totalPageNum;

  public int curUsedPageNum;

  Column[] columns;
  int primaryIndex;
  int KSize, VSize;

  ArrayList<Integer> vacantPage;

  public int rootPageId;

  public PageManager(String databaseName, String tableName, Column[] columns, int primaryIndex) {
    //    this.path = path;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = columns;
    this.primaryIndex = primaryIndex;
    this.curUsedPageNum = 0;
    this.totalPageNum = -1;
    this.vacantPage = new ArrayList<>();
    getKVSize();
    initFile(true);
  }

  public PageManager(
      String databaseName,
      String tableName,
      Column[] columns,
      int primaryIndex,
      Boolean recoverFromFileOrNot) {
    //    this.path = path;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = columns;
    this.primaryIndex = primaryIndex;
    this.curUsedPageNum = 0;
    this.totalPageNum = -1;
    this.vacantPage = new ArrayList<>();
    getKVSize();
    initFile(recoverFromFileOrNot);
  }
  // write read lock
  private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock pageLock = new ReentrantReadWriteLock();
  private static HashMap<String, Integer> bufferIndex = new HashMap<>();
  private static LinkedList<String> bufferLinkedList =
      new LinkedList<>(); // 储存bufferName和对应pool的index

  private static LinkedList<Integer> emptyBufferIndex = new LinkedList<>();

  static {
    for (int i = 0; i < Global.BUFFER_POOL_SIZE; i++) {
      emptyBufferIndex.add(i);
    }
  }

  private static byte[][] bufferPool = new byte[Global.BUFFER_POOL_SIZE][Global.PAGE_SIZE];

  public static void deleteDBBuffer(String databaseName) {
    try {
      lock.writeLock().lock();
      ArrayList<String> toDelete = new ArrayList<>();
      for (String key : bufferIndex.keySet()) {
        if (key.split("@")[0].equals(databaseName)) {
          toDelete.add(key);
        }
      }
      for (String key : toDelete) {
        int index = bufferIndex.get(key);
        emptyBufferIndex.add(index);
        bufferIndex.remove(key);
        bufferLinkedList.remove(key);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public static void deleteTableBuffer(String databaseName, String tableName) {
    try {
      lock.writeLock().lock();
      ArrayList<String> toDelete = new ArrayList<>();
      for (String key : bufferIndex.keySet()) {
        if (key.split("@")[0].equals(databaseName) && key.split("@")[1].equals(tableName)) {
          toDelete.add(key);
        }
      }
      for (String key : toDelete) {
        int index = bufferIndex.get(key);
        emptyBufferIndex.add(index);
        bufferIndex.remove(key);
        bufferLinkedList.remove(key);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  private static byte[] readBuffer(String databaseName, String tableName, int pageId) {
    try {
      lock.writeLock().lock();
      String hashKey = databaseName + "@" + tableName + "@" + pageId;
      Boolean contains = bufferIndex.containsKey(hashKey);
      if (contains) {
        int index = bufferIndex.get(hashKey);
        //        bufferLinkedList.indexOf(hashKey);
        bufferLinkedList.remove(hashKey);
        bufferLinkedList.addFirst(hashKey);
        //        if (index >= Global.BUFFER_POOL_SIZE) {
        //          System.out.println("index out of bound");
        //        }
        return bufferPool[index];
      } else {
        String pathRead = PathUtil.getBinFilePath(databaseName, tableName);
        if (bufferLinkedList.size() == Global.BUFFER_POOL_SIZE) {
          String lastHashKey = bufferLinkedList.removeLast();
          int index = bufferIndex.get(lastHashKey);
          bufferLinkedList.addFirst(hashKey);
          bufferIndex.remove(lastHashKey);
          bufferIndex.put(hashKey, index);
          String pathWrite =
              PathUtil.getBinFilePath(lastHashKey.split("@")[0], lastHashKey.split("@")[1]);
          int pageIdWrite = Integer.parseInt(lastHashKey.split("@")[2]);
          lock.writeLock().unlock();
          try (RandomAccessFile rafRead = new RandomAccessFile(new File(pathRead), "r");
              RandomAccessFile rafWrite = new RandomAccessFile(new File(pathWrite), "rw")) {

            rafWrite.seek(pageIdWrite * Global.PAGE_SIZE);
            rafWrite.write(bufferPool[index]);
            rafRead.seek(pageId * Global.PAGE_SIZE);
            rafRead.read(bufferPool[index]);
            //            if (bufferLinkedList.size() > Global.BUFFER_POOL_SIZE)
            //              System.out.println("bufferLinkedList.size()>=Global.BUFFER_POOL_SIZE");
            return bufferPool[index];
          }
        } else {
          try (RandomAccessFile raf = new RandomAccessFile(new File(pathRead), "r")) {

            int index = -1;
            assert emptyBufferIndex.size() > 0;
            index = emptyBufferIndex.removeFirst();
            if (index == -1) throw new RuntimeException();
            bufferLinkedList.addFirst(hashKey);
            bufferIndex.put(hashKey, index);
            lock.writeLock().unlock();
            raf.seek(pageId * Global.PAGE_SIZE);
            raf.read(bufferPool[index]);
            //            if (bufferLinkedList.size() > Global.BUFFER_POOL_SIZE)
            //              System.out.println("bufferLinkedList.size()>=Global.BUFFER_POOL_SIZE");

            return bufferPool[index];
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    } finally {
      if (lock.writeLock().isHeldByCurrentThread()) lock.writeLock().unlock();
    }
  }

  private static byte[] writeBuffer(String databaseName, String tableName, int pageId, byte[] buf) {
    try {
      lock.writeLock().lock();
      String hashKey = databaseName + "@" + tableName + "@" + pageId;
      Boolean contains = bufferIndex.containsKey(hashKey);
      if (contains) {
        int index = bufferIndex.get(hashKey);
        //        bufferLinkedList.indexOf(hashKey);
        bufferLinkedList.remove(hashKey);
        bufferLinkedList.addFirst(hashKey);
        bufferPool[index] = buf;
        return bufferPool[index];
      } else {
        if (bufferLinkedList.size() == Global.BUFFER_POOL_SIZE) {
          String lastHashKey = bufferLinkedList.removeLast();
          int index = bufferIndex.get(lastHashKey);
          bufferPool[index] = buf;
          bufferLinkedList.addFirst(hashKey);
          bufferIndex.put(hashKey, index);
          bufferIndex.remove(lastHashKey);
          lock.writeLock().unlock();
          String pathWrite =
              PathUtil.getBinFilePath(lastHashKey.split("@")[0], lastHashKey.split("@")[1]);
          int pageIdWrite = Integer.parseInt(lastHashKey.split("@")[2]);
          try (RandomAccessFile rafWrite = new RandomAccessFile(new File(pathWrite), "rw")) {
            rafWrite.seek(pageIdWrite * Global.PAGE_SIZE);
            rafWrite.write(bufferPool[index]);
            //            if (bufferLinkedList.size() > Global.BUFFER_POOL_SIZE)
            //              System.out.println("bufferLinkedList.size()>=Global.BUFFER_POOL_SIZE");

            return bufferPool[index];
          } catch (Exception e) {
            e.printStackTrace();
            return null;
          }
        } else {
          int index = -1;
          assert emptyBufferIndex.size() > 0;
          index = emptyBufferIndex.removeFirst();
          if (index == -1) throw new RuntimeException();
          bufferLinkedList.addFirst(hashKey);
          bufferIndex.put(hashKey, index);
          bufferPool[index] = buf;
          //          if (bufferLinkedList.size() > Global.BUFFER_POOL_SIZE)
          //            System.out.println("bufferLinkedList.size()>=Global.BUFFER_POOL_SIZE");

          return bufferPool[index];
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    } finally {
      if (lock.writeLock().isHeldByCurrentThread()) lock.writeLock().unlock();
    }
  }

  public static void checkPoint() {
    // iter bufferIndex
    try {
      lock.writeLock().lock();
      for (Map.Entry<String, Integer> entry : bufferIndex.entrySet()) {
        String hashKey = entry.getKey();
        int index = entry.getValue();
        String pathWrite = PathUtil.getBinFilePath(hashKey.split("@")[0], hashKey.split("@")[1]);
        int pageIdWrite = Integer.parseInt(hashKey.split("@")[2]);
        try (RandomAccessFile rafWrite = new RandomAccessFile(new File(pathWrite), "rw")) {
          rafWrite.seek(pageIdWrite * Global.PAGE_SIZE);
          rafWrite.write(bufferPool[index]);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public int newPage() {
    try {
      lock.writeLock().lock();

      if (vacantPage.isEmpty()) {
        ++curUsedPageNum;
        try {
          if (curUsedPageNum >= totalPageNum) flush();
          writeHeader();
          return curUsedPageNum;
        } catch (Exception e) {
          e.printStackTrace();
          return -1;
        }
      } else {
        int ret = vacantPage.get(0);
        vacantPage.remove(0);
        return ret;
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void deletePage(int vacant) {
    vacantPage.add(vacant);
  }

  void initFile(Boolean recoverFromFileOrNot) {
    try {
      String path = PathUtil.getBinFilePath(databaseName, tableName);
      File file = new File(path);
      if (recoverFromFileOrNot && file.exists()) {
        this.readHeader();
        if (rootPageId != -1 && rootPageId <= totalPageNum && rootPageId > 0) return;
        System.err.println("File is destroyed, recreate a new file...");
      }
      // No file or destroyed file
      rootPageId = -1;
      if (!file.exists()) {
        Boolean result = file.createNewFile();
        if (!result) throw new RuntimeException();
      }
      flush();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  void readHeader() {
    try {
      //      pageLock.readLock().lock();
      byte b[] = readBuffer(databaseName, tableName, 0);
      if (b == null) {
        rootPageId = -1;
        return;
      }
      ByteBufferReader reader = new ByteBufferReader(b);
      totalPageNum = reader.readInt();
      rootPageId = reader.readInt();
      curUsedPageNum = reader.readInt();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      //      pageLock.readLock().unlock();
    }
  }

  void writeInternalNode(int pageId, BPlusTreeInternalNode node) {
    try {
      //      pageLock.writeLock().lock();
      ByteBufferWriter writer = new ByteBufferWriter(Global.PAGE_SIZE);
      writer.writeChar('I');
      int size = node.nodeSize;
      writer.writeInt(size); // size
      assert 2 + 2 + 2 + size * (KSize + 2) + 2 <= Global.PAGE_SIZE;
      for (int i = 0; i < size; i++) {
        int childPageId = node.readChildFromDisk(i);
        writer.writeInt(childPageId);
        Entry e = (Entry) node.keys.get(i);
        Column keyColumn = columns[primaryIndex];
        writeKey(writer, keyColumn, e);
      }
      writer.writeInt(node.readChildFromDisk(size));
      writeBuffer(databaseName, tableName, pageId, writer.getBuf());
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      //      pageLock.writeLock().unlock();
    }
  }

  public BPlusTreeNode getRootNode() {
    if (rootPageId == -1) {
      try {
        //          pageLock.writeLock().lock();
        rootPageId = this.newPage();
        writeHeader();
        BPlusTreeLeafNode newNode = new BPlusTreeLeafNode<>(0, rootPageId, -1, this);
        writeLeafNode(rootPageId, newNode);
        return (BPlusTreeNode) newNode;
      } finally {
        //          pageLock.writeLock().unlock();
      }
    } else return this.readNode(rootPageId);
  }

  public BPlusTreeNode readNode(int pageId) {
    BPlusTreeNode res = null;
    if (pageId < 0) return null;
    try {
      //        pageLock.readLock().lock();
      byte b[] = readBuffer(databaseName, tableName, pageId);
      ByteBufferReader reader = new ByteBufferReader(b);
      char type = reader.readChar();
      if (type == 'I') res = readInternalNode(reader, pageId);
      else if (type == 'L') res = readLeafNode(reader, pageId);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      //        pageLock.readLock().unlock();
      return res;
    }
  }

  BPlusTreeInternalNode readInternalNode(ByteBufferReader reader, int pageId) throws IOException {
    int size = reader.readInt(); // size
    assert 2 + 2 + 2 + size * (KSize + 2) + 2 <= Global.PAGE_SIZE;
    ArrayList<Entry> k = new ArrayList<>(Collections.nCopies(Global.ARRAY_LIST_MAX_LENGTH, null));
    ArrayList<Integer> children =
        new ArrayList<>(Collections.nCopies(Global.ARRAY_LIST_MAX_LENGTH, null));
    for (int i = 0; i < size; i++) {
      children.set(i, reader.readInt());
      k.set(i, readKey(reader, columns[primaryIndex]));
    }
    children.set(size, reader.readInt());
    return new BPlusTreeInternalNode(size, k, children, pageId, this);
  }

  void getKVSize() {
    VSize = 0;
    for (Column column : columns) {
      ColumnType type = column.getColumnType();
      if (type == ColumnType.STRING) VSize += column.getMaxLength() * 2;
      else if (type == ColumnType.INT || type == ColumnType.FLOAT) VSize += 2;
      else VSize += 4;
    }
    Column primaryColumn = columns[primaryIndex];
    ColumnType primaryType = primaryColumn.getColumnType();
    if (primaryType == ColumnType.STRING) KSize = primaryColumn.getMaxLength() * 2;
    else if (primaryType == ColumnType.INT || primaryType == ColumnType.FLOAT) KSize = 2;
    else KSize = 4;
  }

  void writeHeaderPageNum(RandomAccessFile raf) {
    try {
      raf.seek(0);
      raf.writeInt(totalPageNum);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  void writeHeader() {
    try {
      ByteBufferWriter writer = new ByteBufferWriter(Global.PAGE_SIZE);
      writer.writeInt(totalPageNum);
      writer.writeInt(rootPageId);
      writer.writeInt(curUsedPageNum);
      writeBuffer(databaseName, tableName, 0, writer.getBuf());
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
    }
  }

  void updateRoot(int rtPageId) {
    try {
      pageLock.writeLock().lock();
      this.rootPageId = rtPageId;
      writeHeader();
    } finally {
      pageLock.writeLock().unlock();
    }
  }

  void writeLeafNode(int pageId, BPlusTreeLeafNode node) {
    try {
      //      pageLock.writeLock().lock();
      ByteBufferWriter writer = new ByteBufferWriter(Global.PAGE_SIZE);
      writer.writeChar('L');
      int size = node.nodeSize;
      writer.writeInt(size); // size

      writer.writeInt(node.nextPageId); // next
      assert 2 + 2 + 2 + size * (KSize + VSize) <= Global.PAGE_SIZE;
      for (int i = 0; i < size; i++) {
        Entry e = (Entry) node.keys.get(i);
        Row r = (Row) node.values.get(i);
        Column keyColumn = columns[primaryIndex];
        writeKey(writer, keyColumn, e);
        writeValue(writer, columns, r);
      }
      writeBuffer(databaseName, tableName, pageId, writer.getBuf());
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      //      pageLock.writeLock().unlock();
    }
  }

  BPlusTreeLeafNode readLeafNode(ByteBufferReader reader, int pageId) throws IOException {
    int size = reader.readInt();
    int next = reader.readInt();
    assert 2 + 2 + 2 + size * (KSize + VSize) <= Global.PAGE_SIZE;
    ArrayList<Entry> k = new ArrayList<>(Collections.nCopies(Global.ARRAY_LIST_MAX_LENGTH, null));
    ArrayList<Row> v = new ArrayList<>(Collections.nCopies(Global.ARRAY_LIST_MAX_LENGTH, null));
    for (int i = 0; i < size; i++) {
      k.set(i, readKey(reader, columns[primaryIndex]));
      v.set(i, readValue(reader, columns));
    }
    return new BPlusTreeLeafNode(size, k, v, pageId, next, this);
  }

  void writeKey(ByteBufferWriter writer, Column column, Entry e) throws IOException {
    ColumnType type = column.getColumnType();
    Comparable value;
    if (e.value == null) {
      value =
          (type == ColumnType.INT
              ? Integer.MIN_VALUE
              : type == ColumnType.FLOAT
                  ? Float.MIN_VALUE
                  : type == ColumnType.LONG
                      ? Long.MIN_VALUE
                      : type == ColumnType.DOUBLE ? Double.MIN_VALUE : "");
      writer.writeChar('N');
    } else {
      value = e.value;
      writer.writeChar('V');
    }
    if (type == ColumnType.INT) writer.writeInt((int) value);
    else if (type == ColumnType.FLOAT) writer.writeFloat((float) value);
    else if (type == ColumnType.LONG) writer.writeLong((long) value);
    else if (type == ColumnType.DOUBLE) writer.writeDouble((double) value);
    else {
      String s_value = (String) value;
      writer.writeChars(s_value);
      for (int i = 0; i < column.getMaxLength() - s_value.length(); i++)
        writer.writeChar(0); // padding to the max length
    }
  }

  void writeValue(ByteBufferWriter writer, Column[] columns, Row r) throws IOException {
    for (int i = 0; i < columns.length; i++) {
      writeKey(writer, columns[i], r.getEntries().get(i));
    }
  }

  Row readValue(ByteBufferReader reader, Column[] columns) throws IOException {
    ArrayList<Entry> entries = new ArrayList<>();
    for (int i = 0; i < columns.length; i++) {
      entries.add(readKey(reader, columns[i]));
    }
    return new Row(entries);
  }

  Entry readKey(ByteBufferReader reader, Column column) {
    ColumnType type = column.getColumnType();
    Comparable value;
    char flag = reader.readChar();
    if (flag == 'N') {
      if (type == ColumnType.INT) reader.seek(Integer.BYTES);
      else if (type == ColumnType.FLOAT) reader.seek(Float.BYTES);
      else if (type == ColumnType.LONG) reader.seek(Long.BYTES);
      else if (type == ColumnType.DOUBLE) reader.seek(Double.BYTES);
      else reader.seek(column.getMaxLength() * Character.BYTES);

      return new Entry(null);
    } else if (flag != 'V') {
      throw new RuntimeException();
    }

    //    if (type == ColumnType.FLOAT) {
    //      System.out.println("here");
    //    }
    if (type == ColumnType.INT) value = reader.readInt();
    else if (type == ColumnType.FLOAT) value = reader.readFloat();
    else if (type == ColumnType.LONG) value = reader.readLong();
    else if (type == ColumnType.DOUBLE) value = reader.readDouble();
    else {
      String str = "";
      for (int i = 1; i <= column.getMaxLength(); i++) {
        char x = reader.readChar();
        if (x == 0) {
          reader.seek((column.getMaxLength() - i) * Character.BYTES);
          break;
        }
        str += x;
      }
      value = str;
    }

    return new Entry(value);
  }

  void flush() {
    try {
      //      pageLock.writeLock().lock();
      String path = PathUtil.getBinFilePath(databaseName, tableName);
      File file = new File(path);
      if (totalPageNum == -1) {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
          int length = Global.PAGE_SIZE * Global.INIT_PAGE_NUM;
          raf.setLength(length); // 设置文件的长度为4KB
          raf.write(new byte[length]); // 用空字节数组写满整个文件
          totalPageNum = Global.INIT_PAGE_NUM;
          writeHeaderPageNum(raf);
        } catch (IOException e) {
          e.printStackTrace();
        }
      } else {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
          int length = Global.PAGE_SIZE * totalPageNum;
          raf.setLength(length * 2);
          totalPageNum *= 2;
          writeHeaderPageNum(raf);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      //      pageLock.writeLock().unlock();
    }
  }
}

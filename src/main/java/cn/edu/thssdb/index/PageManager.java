package cn.edu.thssdb.index;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.ByteBufferReader;
import cn.edu.thssdb.utils.ByteBufferWriter;
import cn.edu.thssdb.utils.Global;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;

public class PageManager {
  String path;
  int totalPageNum;

  public int curUsedPageNum;

  Column[] columns;
  int primaryIndex;
  int KSize, VSize;

  ArrayList<Integer> vacantPage;

  public int rootPageId;

  public PageManager(String path, Column[] columns, int primaryIndex) {
    this.path = path;
    this.columns = columns;
    this.primaryIndex = primaryIndex;
    this.curUsedPageNum = 0;
    this.totalPageNum = -1;
    this.vacantPage = new ArrayList<>();
    getKVSize();
    initFile(true);
  }

  public PageManager(String path, Column[] columns, int primaryIndex, Boolean recoverOrNot) {
    this.path = path;
    this.columns = columns;
    this.primaryIndex = primaryIndex;
    this.curUsedPageNum = 0;
    this.totalPageNum = -1;
    this.vacantPage = new ArrayList<>();
    getKVSize();
    initFile(recoverOrNot);
  }

  public int newPage() {
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
  }

  public void deletePage(int vacant) {
    vacantPage.add(vacant);
  }

  void initFile(Boolean recoverOrNot) {
    try {
      File file = new File(path);
      if (recoverOrNot && file.exists()) {
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
    try (RandomAccessFile raf = new RandomAccessFile(new File(path), "r")) {
      raf.seek(0);
      byte b[] = new byte[Global.PAGE_SIZE];
      raf.read(b);
      ByteBufferReader reader = new ByteBufferReader(b);
      totalPageNum = reader.readInt();
      rootPageId = reader.readInt();
      curUsedPageNum = reader.readInt();
    } catch (IOException e) {
      e.printStackTrace();
      rootPageId = -1;
    }
  }

  void writeInternalNode(int pageId, BPlusTreeInternalNode node) {
    try (RandomAccessFile raf = new RandomAccessFile(new File(path), "rw")) {

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

      raf.seek(pageId * Global.PAGE_SIZE);
      raf.write(writer.getBuf());

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public BPlusTreeNode getRootNode() {
    if (rootPageId == -1) {
      rootPageId = this.newPage();
      writeHeader();
      BPlusTreeLeafNode newNode = new BPlusTreeLeafNode<>(0, rootPageId, -1, this);
      writeLeafNode(rootPageId, newNode);
      return (BPlusTreeNode)newNode;
    } else return this.readNode(rootPageId);
  }

  public BPlusTreeNode readNode(int pageId) {
    BPlusTreeNode res = null;
    if (pageId < 0) return null;
    try (RandomAccessFile raf = new RandomAccessFile(new File(path), "r")) {
      raf.seek(pageId * Global.PAGE_SIZE);
      byte b[] = new byte[Global.PAGE_SIZE];
      raf.read(b);
      ByteBufferReader reader = new ByteBufferReader(b);
      char type = reader.readChar();
      if (type == 'I') res = readInternalNode(reader, pageId);
      else if (type == 'L') res = readLeafNode(reader, pageId);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
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
    try (RandomAccessFile raf = new RandomAccessFile(new File(path), "rw")) {
      raf.seek(0);
      raf.writeInt(totalPageNum);
      raf.writeInt(rootPageId);
      raf.writeInt(curUsedPageNum);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  void updateRoot(int rtPageId) {
    this.rootPageId = rtPageId;
    writeHeader();
  }

  void writeLeafNode(int pageId, BPlusTreeLeafNode node) {
    try (RandomAccessFile raf = new RandomAccessFile(new File(path), "rw")) {
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
      raf.seek(pageId * Global.PAGE_SIZE);
      raf.write(writer.getBuf());
    } catch (Exception e) {
      e.printStackTrace();
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
    if (type == ColumnType.INT) writer.writeInt((int) e.value);
    else if (type == ColumnType.FLOAT) writer.writeFloat((float) e.value);
    else if (type == ColumnType.LONG) writer.writeLong((long) e.value);
    else if (type == ColumnType.DOUBLE) writer.writeDouble((double) e.value);
    else {
      String value = (String) e.value;
      writer.writeChars(value);
      for (int i = 0; i < column.getMaxLength() - value.length(); i++)
        writer.writeChar(0); // padding to the max length
    }
  }

  void writeValue(ByteBufferWriter writer, Column[] columns, Row r) throws IOException {
    for (int i = 0; i < columns.length; i++) {
      writeKey(writer, columns[primaryIndex], r.getEntries().get(i));
    }
  }

  Row readValue(ByteBufferReader reader, Column[] columns) throws IOException {
    ArrayList<Entry> entries = new ArrayList<>();
    for (int i = 0; i < columns.length; i++) {
      entries.add(readKey(reader, columns[primaryIndex]));
    }
    return new Row(entries);
  }

  Entry readKey(ByteBufferReader reader, Column column) {
    ColumnType type = column.getColumnType();
    Comparable value;
    if (type == ColumnType.INT) value = reader.readInt();
    else if (type == ColumnType.FLOAT) value = reader.readFloat();
    else if (type == ColumnType.LONG) value = reader.readLong();
    else if (type == ColumnType.DOUBLE) value = reader.readDouble();
    else {
      String str = "";
      for (int i = 1; i <= column.getMaxLength(); i++) {
        char x = reader.readChar();
        if (x == 0) {
          //          raf.seek(raf.getFilePointer() + (column.getMaxLength() - i) * 2);
          reader.seek(reader.getFilePointer() + (column.getMaxLength() - i) * 2);
          break;
        }
        str += x;
      }
      value = str;
    }
    return new Entry(value);
  }

  void flush() {
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
  }
}

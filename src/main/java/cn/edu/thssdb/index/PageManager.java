package cn.edu.thssdb.index;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Global;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;

public class PageManager {
    String path;
    int totalPageNum;

    int curUsedPageNum;

    Column[] columns;
    int primaryIndex;
    int KSize, VSize;

    ArrayList<Integer> vacantPage;


    public PageManager (String path, Column[] columns, int primaryIndex){
        this.path = path;
        this.columns = columns;
        this.primaryIndex = primaryIndex;
        this.curUsedPageNum = 0;
        this.totalPageNum = -1;
        this.vacantPage = new ArrayList<>();
        getKVSize();
        initFile();
    }

    public int newPage() {
        if (vacantPage.isEmpty()) {
            ++curUsedPageNum;
//            System.out.println("USEDPAGENUM" + curUsedPageNum);
            try {
                if (curUsedPageNum >= totalPageNum)
                    flush();
                return curUsedPageNum;
            } catch (Exception e) {
                e.printStackTrace();
                return -1;
            }
        }
        else {
            int ret = vacantPage.get(0);
            vacantPage.remove(0);
            return ret;
        }
    }

    public void deletePage(int vacant) {
        vacantPage.add(vacant);
    }

    void initFile() {
        try {
            File file = new File(path);
            if (!file.exists())  {
                Boolean result =  file.createNewFile();
                if(!result) throw new RuntimeException();
            }
            flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    void writeInternalNode(int pageId, BPlusTreeInternalNode node) {
        try {
            RandomAccessFile raf = new RandomAccessFile(new File(path), "rw");
            raf.seek(pageId * Global.PAGE_SIZE);
            raf.writeChar('I');
            int size = node.nodeSize;
            raf.writeInt(size); // size
            assert 2 + 2 + 2 + size * (KSize + 2) + 2 <= Global.PAGE_SIZE;
            for (int i = 0; i < size; i++){
                int childPageId = node.readChildFromDisk(i);
                raf.writeInt(childPageId);
                Entry e = (Entry)node.keys.get(i);
                Column keyColumn = columns[primaryIndex];
                writeKey(raf, keyColumn, e);
            }
            raf.writeInt(node.readChildFromDisk(size));
            raf.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public BPlusTreeNode readNode(int pageId){
        BPlusTreeNode res = null;
        if (pageId < 0) return null;
        try{
            RandomAccessFile raf = new RandomAccessFile(new File(path), "rw");
            raf.seek(pageId * Global.PAGE_SIZE);
            char type = raf.readChar();
            if (type == 'I')
                res = readInternalNode(raf, pageId);
            else if (type == 'L')
                res = readLeafNode(raf, pageId);
            raf.close();
        } catch(Exception e){
            e.printStackTrace();
        } finally {
            return res;
        }
    }

    BPlusTreeInternalNode readInternalNode(RandomAccessFile raf, int pageId) throws IOException{
            int size = raf.readInt(); // size
            assert 2 + 2 + 2 + size * (KSize + 2) + 2 <= Global.PAGE_SIZE;
            ArrayList<Entry> k = new ArrayList<>(Collections.nCopies(Global.ARRAY_LIST_MAX_LENGTH, null));
            ArrayList<Integer> children = new ArrayList<>(Collections.nCopies(Global.ARRAY_LIST_MAX_LENGTH, null));
            for (int i = 0; i < size; i++){
                children.set(i, raf.readInt());
                k.set(i, readKey(raf, columns[primaryIndex]));
            }
            children.set(size, raf.readInt());
            return new BPlusTreeInternalNode(size, k, children, pageId, this);

    }
    void getKVSize(){
        VSize = 0;
        for (Column column : columns){
            ColumnType type = column.getColumnType();
            if (type == ColumnType.STRING)
                VSize += column.getMaxLength() * 2;
            else if (type == ColumnType.INT || type == ColumnType.FLOAT)
                VSize += 2;
            else
                VSize += 4;
        }
        Column primaryColumn = columns[primaryIndex];
        ColumnType primaryType = primaryColumn.getColumnType();
        if (primaryType == ColumnType.STRING)
            KSize = primaryColumn.getMaxLength() * 2;
        else if (primaryType == ColumnType.INT || primaryType == ColumnType.FLOAT)
            KSize = 2;
        else
            KSize = 4;
    }

    void writeHeaderPageNum(RandomAccessFile raf) {
        try {
            raf.seek(0);
            raf.writeInt(totalPageNum);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void writeLeafNode(int pageId, BPlusTreeLeafNode node) {
        try {
            RandomAccessFile raf = new RandomAccessFile(new File(path), "rw");
            raf.seek(pageId * Global.PAGE_SIZE);
            raf.writeChar('L');
            int size = node.nodeSize;
            raf.writeInt(size); // size

            raf.writeInt(node.nextPageId); // next
//            System.out.println("BTree size" + size);
            assert 2 + 2 + 2 + size * (KSize + VSize) <= Global.PAGE_SIZE;
            for (int i = 0; i < size; i++) {
                Entry e = (Entry) node.keys.get(i);
//                System.out.println("BTree entry" + e);
                Row r = (Row) node.values.get(i);
                Column keyColumn = columns[primaryIndex];
                writeKey(raf, keyColumn, e);
                writeValue(raf, columns, r);
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    BPlusTreeLeafNode readLeafNode(RandomAccessFile raf, int pageId) throws IOException{
            raf.seek(pageId * Global.PAGE_SIZE);
            char type = raf.readChar();
            assert type == 'L';
            int size = raf.readInt();
            int next = raf.readInt();
            assert  2 + 2 + 2 + size * (KSize + VSize) <= Global.PAGE_SIZE;
            ArrayList<Entry> k = new ArrayList<>(Collections.nCopies(Global.ARRAY_LIST_MAX_LENGTH, null));
            ArrayList<Row> v = new ArrayList<>(Collections.nCopies(Global.ARRAY_LIST_MAX_LENGTH, null));
            for (int i = 0; i < size; i++) {
                k.set(i, readKey(raf, columns[primaryIndex]));
                v.set(i, readValue(raf, columns));
            }
            return new BPlusTreeLeafNode(size, k, v, pageId, next, this);
    }



    void writeKey(RandomAccessFile raf, Column column, Entry e) throws IOException{
        ColumnType type = column.getColumnType();
        if (type == ColumnType.INT)
            raf.writeInt((int)e.value);
        else if (type == ColumnType.FLOAT)
            raf.writeFloat((float) e.value);
        else if (type == ColumnType.LONG)
            raf.writeLong((long) e.value);
        else if (type == ColumnType.DOUBLE)
            raf.writeDouble((double) e.value);
        else {
            String value = (String) e.value;
            raf.writeChars(value);
            for (int i = 0; i < column.getMaxLength() - value.length(); i++)
                raf.writeChar(0); //padding to the max length
        }
    }

    void writeValue(RandomAccessFile raf, Column[] columns, Row r)  throws IOException{
        for (int i = 0; i < columns.length; i++){
            writeKey(raf, columns[primaryIndex], r.getEntries().get(i));
        }
    }

    Row readValue(RandomAccessFile raf, Column[] columns)  throws IOException{
        ArrayList<Entry> entries = new ArrayList<>();
        for (int i = 0; i < columns.length; i++){
            entries.add(readKey(raf, columns[primaryIndex]));
        }
        return new Row(entries);
    }


    Entry readKey(RandomAccessFile raf, Column column) throws IOException {
        ColumnType type = column.getColumnType();
        Comparable value;
        if (type == ColumnType.INT)
            value = raf.readInt();
        else if (type == ColumnType.FLOAT)
            value = raf.readFloat();
        else if (type == ColumnType.LONG)
            value = raf.readLong();
        else if (type == ColumnType.DOUBLE)
            value = raf.readDouble();
        else {
            String str = "";
            for (int i = 1; i <= column.getMaxLength(); i++) {
                char x = raf.readChar();
                if (x == 0) {
                    raf.seek(raf.getFilePointer() + (column.getMaxLength() - i) * 2);
                    break;
                }
                str += x;
            }
            value = str;
        }
        return new Entry(value);
    }

    void flush() throws FileNotFoundException{
        File file = new File(path);
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        if(totalPageNum == -1) {
            try {
                int length = Global.PAGE_SIZE * Global.INIT_PAGE_NUM;
                raf.setLength(length); // 设置文件的长度为4KB
                raf.write(new byte[length]); // 用空字节数组写满整个文件
                totalPageNum = Global.INIT_PAGE_NUM;
                writeHeaderPageNum(raf);
                raf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        else {
            try {
                int length = Global.PAGE_SIZE * totalPageNum;
                raf.setLength(length * 2);
//                raf.seek(length);
//                byte[] buffer = new byte[length];
//                raf.read(buffer);
//                raf.write(buffer);
                totalPageNum *= 2;
                writeHeaderPageNum(raf);
                raf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

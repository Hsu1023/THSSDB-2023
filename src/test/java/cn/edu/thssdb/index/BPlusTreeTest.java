package cn.edu.thssdb.index;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.ColumnType;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BPlusTreeTest {
  private BPlusTree<Entry, Row> tree;
  private ArrayList<Entry> keys;
  private ArrayList<Row> values;
  private HashMap<Integer, Integer> map;

  Entry int2Entry(int i){
    return new Entry(i);
  }
  Row int2Row(int i){
    ArrayList<Entry> entries = new ArrayList<>();
    entries.add(int2Entry(i));
    return new Row(entries);
  }

  int entry2Int(Entry e){
    return (int)e.value;
  }
  int row2Int(Row r){
    return (int)r.getEntries().get(0).value;
  }

  @Before
  public void setUp() {
    keys = new ArrayList<>();
    values = new ArrayList<>();
    map = new HashMap<>();
    HashSet<Integer> set = new HashSet<>();
    int size = 1000;
    Column [] columns = new Column[1];
    columns[0] = new Column("", ColumnType.INT, 0, true, 2);

    File file = new File("./data.bin");
    if (!file.exists()) try{file.createNewFile();}catch (Exception e){e.printStackTrace();}
    tree = new BPlusTree<>("./data.bin", columns, 0);

    for (int i = 0; i < size; i++) {
      double random = Math.random();
      set.add((int) (random * size));
    }
//    set.add(5);set.add(3);set.add(4);set.add(8);set.add(6);set.add(7);set.add(9);
    System.out.println("TOTAL" + set.size());
    for (Integer key : set) {
      int hashCode = key.hashCode();
      keys.add(int2Entry(key));
      values.add(int2Row(hashCode));
      tree.put(int2Entry(key), int2Row(hashCode));
      map.put(key, hashCode);
    }
  }

  @Test
  public void testGet() {
    for (Entry key : keys) {
      int x = map.get(entry2Int(key));
//      System.out.println();
//      System.out.println(tree.size);
      System.out.println(key);
      int y = row2Int( tree.get(key));
      // check if all the results equal
      assertEquals(x, y);
    }
  }

  @Test
  public void testRemove() {
    int size = keys.size();
    for (int i = 0; i < size; i += 2) {
      // remove half data
      tree.remove(keys.get(i));
    }
    // check if size equals half of origin
    assertEquals(size / 2, tree.size());
    for (int i = 1; i < size; i += 2) {
//      assertEquals(, tree.get(keys.get(i)));
      int x = map.get(entry2Int(keys.get(i)));
      int y = row2Int (tree.get(keys.get(i)));
      assertEquals(x, y);
    }
  }

  @Test
  public void testIterator() {
    BPlusTreeIterator<Entry, Row> iterator = tree.iterator();
    int c = 0;
    while (iterator.hasNext()) {
      assertTrue(values.contains(iterator.next().right));
      c++;
    }
    assertEquals(values.size(), c);
  }
}

package cn.edu.thssdb.index;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.ColumnType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class BPlusTreeTest {
  private BPlusTree<Entry, Row> tree;
  private ArrayList<Entry> keys;
  private ArrayList<Row> values;
  private HashMap<Integer, Integer> map;

  private int cnt = 0;

  Entry int2Entry(int i) {
    return new Entry(i);
  }

  Row int2Row(int i) {
    ArrayList<Entry> entries = new ArrayList<>();
    entries.add(int2Entry(i));
    return new Row(entries);
  }

  int entry2Int(Entry e) {
    return (int) e.value;
  }

  int row2Int(Row r) {
    return (int) r.getEntries().get(0).value;
  }

  @Before
  public void setUp() {
    keys = new ArrayList<>();
    values = new ArrayList<>();
    map = new HashMap<>();
    HashSet<Integer> set = new HashSet<>();
    int size = 100000;
    Column[] columns = new Column[1];
    columns[0] = new Column("", ColumnType.INT, 1, true, 2);

    tree = new BPlusTree<>("..", ".", columns, 0, false);

//    tree = new BPlusTree<>();
    for (int i = 0; i < size; i++) {
      double random = Math.random();
      set.add((int) (random * size));
    }
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
      int y = row2Int(tree.get(key));
      // check if all the results equal
      Assert.assertEquals(x, y);
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
    Assert.assertEquals(size / 2, tree.size());
    for (int i = 1; i < size; i += 2) {
      int x = map.get(entry2Int(keys.get(i)));
      int y = row2Int(tree.get(keys.get(i)));
      Assert.assertEquals(x, y);
    }
  }

  @Test
  public void testIterator() {
    BPlusTreeIterator<Entry, Row> iterator = tree.iterator();
    int c = 0;
    ArrayList<Integer> _v = new ArrayList<>();
    for (Row value : values) {
      _v.add((int) value.getEntries().get(0).value);
    }
    while (iterator.hasNext()) {
      int x = (int) iterator.next().right.getEntries().get(0).value;
      Assert.assertTrue(_v.contains(x));
      c++;
    }
    //    System.out.println(_v);
    Assert.assertEquals(values.size(), c);
  }
}

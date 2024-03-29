package cn.edu.thssdb.index;

import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.utils.Global;

import java.util.ArrayList;
import java.util.Collections;

public class BPlusTreeLeafNode<K extends Comparable<K>, V> extends BPlusTreeNode<K, V> {

  ArrayList<V> values;
  //  private BPlusTreeLeafNode<K, V> next;

  public int nextPageId;

  //  BPlusTreeLeafNode(int size) {
  //    keys = new ArrayList<>(Collections.nCopies((int) (1.5 * Global.fanout) + 1, null));
  //    values = new ArrayList<>(Collections.nCopies((int) (1.5 * Global.fanout) + 1, null));
  //    nodeSize = size;
  ////    this.pageId = pageId;
  //  }

  public BPlusTreeLeafNode(
      int size,
      ArrayList<K> k,
      ArrayList<V> v,
      int pageId,
      int nextPageId,
      PageManager pageManager) {
    keys = k;
    values = v;
    nodeSize = size;
    this.nextPageId = nextPageId;
    this.pageId = pageId;
    this.pageManager = pageManager;
  }

  public BPlusTreeLeafNode(int size, int pageId, int nextPageId, PageManager pageManager) {
    keys = new ArrayList<>(Collections.nCopies(Global.ARRAY_LIST_MAX_LENGTH, null));
    values = new ArrayList<>(Collections.nCopies(Global.ARRAY_LIST_MAX_LENGTH, null));
    nodeSize = size;
    this.nextPageId = nextPageId;
    this.pageId = pageId;
    this.pageManager = pageManager;
  }

  public void writeThisToDist() {
    pageManager.writeLeafNode(pageId, this);
  }

  private void valuesAdd(int index, V value) {
    for (int i = nodeSize; i > index; i--) values.set(i, values.get(i - 1));
    values.set(index, value);
  }

  private void valuesRemove(int index) {
    for (int i = index; i < nodeSize - 1; i++) values.set(i, values.get(i + 1));
  }

  @Override
  boolean containsKey(K key) {
    return binarySearch(key) >= 0;
  }

  @Override
  V get(K key) {
    int index = binarySearch(key);
    if (index >= 0) return values.get(index);
    throw new KeyNotExistException();
  }

  @Override
  void put(K key, V value) {
    int index = binarySearch(key);
    int valueIndex = index >= 0 ? index : -index - 1;
    if (index >= 0) {
      //      throw new DuplicateKeyException();
    } else {
      valuesAdd(valueIndex, value);
      keysAdd(valueIndex, key);
    }
    this.writeThisToDist();
  }

  @Override
  void remove(K key) {
    int index = binarySearch(key);
    if (index >= 0) {
      valuesRemove(index);
      keysRemove(index);
    } else {
      System.err.println(key);
      //      throw new KeyNotExistException();
    }
    this.writeThisToDist();
  }

  @Override
  K getFirstLeafKey() {
    return keys.get(0);
  }

  @Override
  BPlusTreeNode<K, V> split() {
    int from = (size() + 1) / 2;
    int to = size();
    BPlusTreeLeafNode<K, V> newSiblingNode =
        new BPlusTreeLeafNode<>(to - from, pageManager.newPage(), -1, pageManager);

    //    System.out.println("NEW LEAF");
    for (int i = 0; i < to - from; i++) {
      newSiblingNode.keys.set(i, keys.get(i + from));
      newSiblingNode.values.set(i, values.get(i + from));
      keys.set(i + from, null);
      values.set(i + from, null);
    }
    nodeSize = from;
    newSiblingNode.nextPageId = nextPageId;
    nextPageId = newSiblingNode.pageId;
    newSiblingNode.writeThisToDist();
    return newSiblingNode;
  }

  @Override
  void merge(BPlusTreeNode<K, V> sibling) {
    int index = size();
    BPlusTreeLeafNode<K, V> node = (BPlusTreeLeafNode<K, V>) sibling;
    int length = node.size();
    for (int i = 0; i < length; i++) {
      keys.set(i + index, node.keys.get(i));
      values.set(i + index, node.values.get(i));
    }
    nodeSize = index + length;
    nextPageId = node.nextPageId;
  }
}

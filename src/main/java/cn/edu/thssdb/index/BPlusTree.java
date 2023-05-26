package cn.edu.thssdb.index;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.utils.Pair;

import java.util.ArrayList;

public final class BPlusTree<K extends Comparable<K>, V> implements Iterable<Pair<K, V>> {

  BPlusTreeNode<K, V> root;

  String path;


//  int K_size = -1;
  public int size;

  PageManager pageManager;

  public BPlusTree(String path, Column[] columns, int primaryIndex) {
    this.pageManager = new PageManager(path, columns, primaryIndex);
    this.root = new BPlusTreeLeafNode<>(0, pageManager.newPage(), -1, pageManager);
    this.path = path;
  }

  public int size() {
    return size;
  }

  public V get(K key) {
    if (key == null) throw new IllegalArgumentException("argument key to get() is null");
    return root.get(key);

  }

  public void put(K key, V value) {
    if (key == null) throw new IllegalArgumentException("argument key to put() is null");
    root.put(key, value);
    size++;
    checkRoot();
  }

  public void remove(K key) {
    if (key == null) throw new IllegalArgumentException("argument key to remove() is null");
    root.remove(key);
    size--;
    if (root instanceof BPlusTreeInternalNode && root.size() == 0) {
//      root = ((BPlusTreeInternalNode<K, V>) root).children.get(0);
      root = ((BPlusTreeInternalNode<K, V>) root).getChildNode(0);
    }
    root.writeThisToDist();

  }

  public boolean contains(K key) {
    if (key == null) throw new IllegalArgumentException("argument key to contains() is null");
    return root.containsKey(key);
  }

  private void checkRoot() {
    if (root.isOverFlow()) {
      BPlusTreeNode<K, V> newSiblingNode = root.split();
      root.writeThisToDist();
      BPlusTreeInternalNode<K, V> newRoot = new BPlusTreeInternalNode<>(1, pageManager.newPage(), pageManager);
      newRoot.keys.set(0, newSiblingNode.getFirstLeafKey());
      newRoot.childrenPageId.set(0, root.pageId);
      newRoot.childrenPageId.set(1, newSiblingNode.pageId);
      assert newRoot.pageId != root.pageId;
      assert newRoot.pageId != newSiblingNode.pageId;
      newRoot.writeThisToDist();
      root = newRoot;
    }
    else {
      root.writeThisToDist();
    }
  }

  @Override
  public BPlusTreeIterator<K, V> iterator() {
    return new BPlusTreeIterator<>(this);
  }
}

package cn.edu.thssdb.index;

import cn.edu.thssdb.utils.Global;

import java.util.ArrayList;
import java.util.Collections;

public final class BPlusTreeInternalNode<K extends Comparable<K>, V> extends BPlusTreeNode<K, V> {

  //  ArrayList<BPlusTreeNode<K, V>> children;

  ArrayList<Integer> childrenPageId;

  //  BPlusTreeInternalNode(int size) {
  //    keys = new ArrayList<>(Collections.nCopies((int) (1.5 * Global.fanout) + 1, null));
  //    children = new ArrayList<>((Collections.nCopies((int) (1.5 * Global.fanout) + 2, null)));
  //    this.nodeSize = size;
  //  }

  public BPlusTreeInternalNode(
      int size, ArrayList<K> k, ArrayList<Integer> children, int pageId, PageManager pageManager) {
    this.keys = k;
    this.childrenPageId = children;
    this.nodeSize = size;
    this.pageManager = pageManager;
    this.pageId = pageId;
  }

  public BPlusTreeInternalNode(int size, int pageId, PageManager pageManager) {
    keys = new ArrayList<>(Collections.nCopies(Global.ARRAY_LIST_MAX_LENGTH, null));
    childrenPageId = new ArrayList<>(Collections.nCopies(Global.ARRAY_LIST_MAX_LENGTH, null));
    nodeSize = size;
    this.pageId = pageId;
    this.pageManager = pageManager;
  }

  public int readChildFromDisk(int i) {
    return childrenPageId.get(i);
  }

  public void writeThisToDist() {
    pageManager.writeInternalNode(pageId, this);
  }

  private void childrenAdd(int index, int newNodePageId) {
    for (int i = nodeSize + 1; i > index; i--) {
      childrenPageId.set(i, childrenPageId.get(i - 1));
    }
    childrenPageId.set(index, newNodePageId);
    assert pageId != newNodePageId;
  }

  private void childrenRemove(int index) {
    pageManager.deletePage(childrenPageId.get(index));
    for (int i = index; i < nodeSize; i++) {
      childrenPageId.set(i, childrenPageId.get(i + 1));
    }
  }

  @Override
  boolean containsKey(K key) {
    return searchChild(key).containsKey(key);
  }

  @Override
  V get(K key) {
    return searchChild(key).get(key);
  }

  @Override
  void put(K key, V value) {
    BPlusTreeNode<K, V> child = searchChild(key);
    child.put(key, value);
    if (child.isOverFlow()) {
      BPlusTreeNode<K, V> newSiblingNode = child.split();

      insertChild(newSiblingNode.getFirstLeafKey(), newSiblingNode);
      this.writeThisToDist();
      child.writeThisToDist();
    }
  }

  @Override
  void remove(K key) {
    int index = binarySearch(key);
    // 如果要查找的元素在列表中的索引为 i，则返回值为 i；如果元素不在列表中，则返回值为“(-(插入点) - 1)”，其中插入点是将元素插入列表中时的索引。
    int childIndex = index >= 0 ? index + 1 : -index - 1;
    BPlusTreeNode<K, V> child = getChildNode(childIndex);
    child.remove(key); // recursive
    if (child.isUnderFlow()) {
      BPlusTreeNode<K, V> childLeftSibling = getChildLeftSibling(key);
      BPlusTreeNode<K, V> left = null;
      BPlusTreeNode<K, V> right = null;
      //      BPlusTreeNode<K, V> left = childLeftSibling != null ? childLeftSibling : child;
      //      BPlusTreeNode<K, V> right = childLeftSibling != null ? child : childRightSibling;
      if (childLeftSibling == null) {
        BPlusTreeNode<K, V> childRightSibling = getChildRightSibling(key);
        left = child;
        right = childRightSibling;
      } else {
        left = childLeftSibling;
        right = child;
      }
      left.merge(right);
      if (index >= 0) { // 如果 key 是指针
        childrenRemove(index + 1);
        keysRemove(index);
      } else { // 如果 key 不是指针
        assert right != null;
        deleteChild(right.getFirstLeafKey());
        right = null;
      }
      if (left.isOverFlow()) {
        BPlusTreeNode<K, V> newSiblingNode = left.split();
        insertChild(newSiblingNode.getFirstLeafKey(), newSiblingNode);
      }
      if (left != null) left.writeThisToDist();
      if (right != null) right.writeThisToDist();
    }
    //    else if (index >= 0) keys.set(index, children.get(index + 1).getFirstLeafKey());
    else if (index >= 0) {
      keys.set(index, (K) (getChildNode(index + 1).getFirstLeafKey()));
    }
    this.writeThisToDist();
  }

  BPlusTreeNode getChildNode(int childIndex) {
    return pageManager.readNode(childrenPageId.get(childIndex));
  }

  @Override
  K getFirstLeafKey() {
    //    return children.get(0).getFirstLeafKey();
    return (K) (getChildNode(0).getFirstLeafKey());
  }

  @Override
  BPlusTreeNode<K, V> split() {
    int from = size() / 2 + 1;
    int to = size();
    BPlusTreeInternalNode<K, V> newSiblingNode =
        new BPlusTreeInternalNode<>(to - from, pageManager.newPage(), pageManager);
    //    System.out.println("NEW INTERNAL");
    for (int i = 0; i < to - from; i++) {
      newSiblingNode.keys.set(i, keys.get(i + from));
      //      newSiblingNode.children.set(i, children.get(i + from));
      newSiblingNode.childrenPageId.set(i, childrenPageId.get(i + from));
    }

    //    newSiblingNode.children.set(to - from, children.get(to));
    newSiblingNode.childrenPageId.set(to - from, childrenPageId.get(to));
    this.nodeSize = this.nodeSize - to + from - 1;
    newSiblingNode.writeThisToDist();
    return newSiblingNode;
  }

  @Override
  void merge(BPlusTreeNode<K, V> sibling) {
    int index = nodeSize;
    BPlusTreeInternalNode<K, V> node = (BPlusTreeInternalNode<K, V>) sibling;
    int length = node.nodeSize;
    keys.set(index, node.getFirstLeafKey());
    for (int i = 0; i < length; i++) {
      keys.set(i + index + 1, node.keys.get(i));
      //      children.set(i + index + 1, node.children.get(i));
      childrenPageId.set(i + index + 1, node.childrenPageId.get(i));
      assert pageId != node.childrenPageId.get(i);
    }
    //    children.set(length + index + 1, node.children.get(length));
    childrenPageId.set(length + index + 1, node.childrenPageId.get(length));
    nodeSize = index + length + 1;
  }

  private BPlusTreeNode<K, V> searchChild(K key) {
    int index = binarySearch(key);
    BPlusTreeNode child = getChildNode(index >= 0 ? index + 1 : -index - 1);
    //    if (child == null) {
    //      System.out.println("child is null");
    //    }
    getChildNode(index >= 0 ? index + 1 : -index - 1);
    return child;
  }

  private void insertChild(K key, BPlusTreeNode<K, V> child) {
    int index = binarySearch(key);
    int childIndex = index >= 0 ? index + 1 : -index - 1;
    if (index >= 0) {
      //      children.set(childIndex, child);
      childrenPageId.set(childIndex, child.pageId);
      assert pageId != child.pageId;
    } else {
      childrenAdd(childIndex + 1, child.pageId);
      keysAdd(childIndex, key);
    }
  }

  private void deleteChild(K key) {
    int index = binarySearch(key);
    if (index >= 0) {
      childrenRemove(index + 1);
      keysRemove(index);
    }
  }

  private BPlusTreeNode<K, V> getChildLeftSibling(K key) {
    int index = binarySearch(key);
    int childIndex = index >= 0 ? index + 1 : -index - 1;
    if (childIndex > 0) return getChildNode(childIndex - 1);
    //      return children.get(childIndex - 1);
    return null;
  }

  private BPlusTreeNode<K, V> getChildRightSibling(K key) {
    int index = binarySearch(key);
    int childIndex = index >= 0 ? index + 1 : -index - 1;
    if (childIndex < size()) return getChildNode(childIndex + 1);
    //      return children.get(childIndex + 1);
    return null;
  }
}

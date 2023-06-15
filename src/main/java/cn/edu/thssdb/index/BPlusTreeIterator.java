package cn.edu.thssdb.index;

import cn.edu.thssdb.utils.Pair;

import java.util.Iterator;
import java.util.LinkedList;

public class BPlusTreeIterator<K extends Comparable<K>, V> implements Iterator<Pair<K, V>> {
  //  private final LinkedList<BPlusTreeNode<K, V>> queue;
  private final LinkedList<Pair<K, V>> buffer;

  BPlusTreeNode<K, V> cursor;

  BPlusTreeIterator(BPlusTree<K, V> tree) {
    buffer = new LinkedList<>();
    //    if (tree.size() == 0) return;
    cursor = tree.root;
    while (true) {
      if (cursor instanceof BPlusTreeLeafNode) {
        for (int i = 0; i < cursor.size(); i++)
          buffer.add(
              new Pair<>(cursor.keys.get(i), ((BPlusTreeLeafNode<K, V>) cursor).values.get(i)));
        cursor = cursor.pageManager.readNode(((BPlusTreeLeafNode<K, V>) cursor).nextPageId);
        return;
      } else cursor = ((BPlusTreeInternalNode<K, V>) cursor).getChildNode(0);
    }
  }

  @Override
  public boolean hasNext() {
    return (cursor != null) || (!buffer.isEmpty());
  }

  @Override
  public Pair<K, V> next() {
    if (buffer.isEmpty()) {
      if (cursor == null) return null;
      for (int i = 0; i < cursor.size(); i++)
        buffer.add(
            new Pair<>(cursor.keys.get(i), ((BPlusTreeLeafNode<K, V>) cursor).values.get(i)));
      cursor = cursor.pageManager.readNode(((BPlusTreeLeafNode<K, V>) cursor).nextPageId);
    }
    return buffer.poll();
  }
}

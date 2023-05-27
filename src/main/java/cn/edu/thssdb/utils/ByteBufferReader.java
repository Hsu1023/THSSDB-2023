package cn.edu.thssdb.utils;

import java.nio.ByteBuffer;

public class ByteBufferReader {

  private byte[] buffer;
  private int cursor;

  public ByteBufferReader(byte[] buffer) {
    this.buffer = buffer;
    this.cursor = 0;
  }

  public int readInt() {
    int value = ByteBuffer.wrap(buffer, cursor, Integer.BYTES).getInt();
    cursor += Integer.BYTES;
    return value;
  }

  public double readDouble() {
    double value = ByteBuffer.wrap(buffer, cursor, Double.BYTES).getDouble();
    cursor += Double.BYTES;
    return value;
  }

  public long readLong() {
    long value = ByteBuffer.wrap(buffer, cursor, Long.BYTES).getLong();
    cursor += Long.BYTES;
    return value;
  }

  public char readChar() {
    char value = ByteBuffer.wrap(buffer, cursor, Character.BYTES).getChar();
    cursor += Character.BYTES;
    return value;
  }

  public float readFloat() {
    float value = ByteBuffer.wrap(buffer, cursor, Float.BYTES).getChar();
    cursor += Float.BYTES;
    return value;
  }

  // 其他读取方法，如readLong、readFloat等

  public void seek(int offset) {
    cursor += offset;
  }

  public int getFilePointer() {
    return cursor;
  }
}

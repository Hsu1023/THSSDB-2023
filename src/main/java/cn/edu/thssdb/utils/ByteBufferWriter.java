package cn.edu.thssdb.utils;

import java.nio.ByteBuffer;

public class ByteBufferWriter {
  private byte[] buffer;
  private int cursor;

  public ByteBufferWriter(int capacity) {
    this.buffer = new byte[capacity];
    this.cursor = 0;
  }

  public void writeInt(int value) {
    ByteBuffer.wrap(buffer, cursor, Integer.BYTES).putInt(value);
    cursor += Integer.BYTES;
  }

  public void writeDouble(double value) {
    ByteBuffer.wrap(buffer, cursor, Double.BYTES).putDouble(value);
    cursor += Double.BYTES;
  }

  public void writeLong(long value) {
    ByteBuffer.wrap(buffer, cursor, Long.BYTES).putLong(value);
    cursor += Long.BYTES;
  }

  public void writeChar(char value) {
    ByteBuffer.wrap(buffer, cursor, Character.BYTES).putChar(value);
    cursor += Character.BYTES;
  }

  public void writeChar(int value) {
    ByteBuffer.wrap(buffer, cursor, Character.BYTES).putChar((char) value);
    cursor += Character.BYTES;
  }

  public void writeFloat(float value) {
    ByteBuffer.wrap(buffer, cursor, Float.BYTES).putFloat(value);
    cursor += Float.BYTES;
  }

  public void writeChars(String value) {
    for (int i = 0; i < value.length(); i++) {
      ByteBuffer.wrap(buffer, cursor, Character.BYTES).putChar(value.charAt(i));
      cursor += Character.BYTES;
    }
  }

  public byte[] getBuf() {
    return buffer;
  }

  // 其他写入方法，如writeLong、writeFloat等

  public void moveCursor(int offset) {
    cursor += offset;
  }

  public byte[] getBuffer() {
    return buffer;
  }
}

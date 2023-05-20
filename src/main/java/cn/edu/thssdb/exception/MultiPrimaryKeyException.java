package cn.edu.thssdb.exception;

public class MultiPrimaryKeyException extends RuntimeException {
  @Override
  public String getMessage() {
    return "Exception: The table has multi primary keys!";
  }
}

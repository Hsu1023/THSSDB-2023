package cn.edu.thssdb.exception;

public class NoPrimaryKeyException extends RuntimeException {
  @Override
  public String getMessage() {
    return "Exception: The table has no primary key!";
  }
}

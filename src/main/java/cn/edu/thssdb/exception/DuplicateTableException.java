package cn.edu.thssdb.exception;

public class DuplicateTableException extends RuntimeException {
  @Override
  public String getMessage() {
    return "Exception: create table caused duplicated tables!";
  }
}

package cn.edu.thssdb.exception;

public class RowNotExistException extends RuntimeException {
  @Override
  public String getMessage() {
    return "Exception: Row doesn't exist!";
  }
}

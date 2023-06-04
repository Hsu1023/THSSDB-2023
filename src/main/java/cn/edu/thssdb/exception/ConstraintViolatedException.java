package cn.edu.thssdb.exception;

public class ConstraintViolatedException extends RuntimeException {
  @Override
  public String getMessage() {
    return "Exception: Constraint violated";
  }
}

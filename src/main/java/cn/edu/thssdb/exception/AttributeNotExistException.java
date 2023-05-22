package cn.edu.thssdb.exception;

public class AttributeNotExistException extends RuntimeException {
  @Override
  public String getMessage() {
    return "Exception: Attribute Not Exist in Condition Sentence";
  }
}

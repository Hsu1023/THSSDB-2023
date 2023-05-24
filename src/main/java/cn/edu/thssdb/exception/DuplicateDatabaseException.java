package cn.edu.thssdb.exception;

public class DuplicateDatabaseException extends RuntimeException {
  @Override
  public String getMessage() {
    return "Exception: create database caused duplicated databases!";
  }
}

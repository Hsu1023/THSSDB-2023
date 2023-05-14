package cn.edu.thssdb.schema;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.FileHandler;

public class Manager {
  private HashMap<String, Database> databases;

  private Database curDatabase;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    // TODO
  }

  private void createDatabaseIfNotExists(String name) {
    // TODO
  }

  private void deleteDatabase() {
    // TODO
  }

  public void switchDatabase() {
    // TODO
  }

  public void persist() {

  }

  private void recover() {

  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();

    private ManagerHolder() {}
  }
}

package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DuplicateKeyException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.utils.Global;

import javax.xml.crypto.Data;
import java.io.File;
import java.util.ArrayList;
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
    databases = new HashMap<>();
    curDatabase = null;
    File dataFile = new File(Global.DATA_PATH);
    if (!dataFile.exists()){
      dataFile.mkdirs();
    }
    this.recover();
  }

  public void createDatabaseIfNotExists(String name) {
    // TODO
    lock.writeLock().lock();
    try{
        if (!databases.containsKey(name)) {
          Database newDatabase = new Database(name);
          databases.put(name, newDatabase);
          System.out.println("[DEBUG] " + "create db "+name);
        }
        else {
          System.out.println("[DEBUG] " + "duplicated db "+name);
          throw new DuplicateKeyException();
        }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void deleteDatabase(String name) {
    // TODO

    lock.writeLock().lock();
    try{
      if (databases.containsKey(name)){
        Database db = databases.get(name);
        // TODO: db ...
        databases.remove(name);
        System.out.println("[DEBUG] " + "delete db "+name);
      }
      else {

        System.out.println("[DEBUG] " + "non-existed db "+name);
        throw new KeyNotExistException();
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public ArrayList<String> getDatabaseNames(){
    return new ArrayList<>(databases.keySet());
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

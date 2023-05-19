package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DuplicateKeyException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.utils.Global;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.FileHandler;

public class Manager {
  private HashMap<String, Database> databases;

  private Database curDatabase;

  private static String MANAGER_DATAPATH = Global.DATA_PATH + File.separator + "manager.db";
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    // TODO
    databases = new HashMap<>();
    curDatabase = null;
    File dataFile = new File(MANAGER_DATAPATH);
    if (!dataFile.exists()){
      dataFile.getParentFile().mkdirs();
    }
    this.recover();
  }

  public void createDatabaseIfNotExists(String name) {
    // TODO
    Boolean change = false;
    lock.writeLock().lock();
    try{
        if (!databases.containsKey(name)) {
          Database newDatabase = new Database(name);
          databases.put(name, newDatabase);
          change = true;
          System.out.println("[DEBUG] " + "create db "+name);
        }
        else {
          System.out.println("[DEBUG] " + "duplicated db "+name);
          throw new DuplicateKeyException();
        }
    } finally {
      if (change) persistDatabases();
      lock.writeLock().unlock();
    }
  }

  public void deleteDatabase(String name) {
    // TODO

    Boolean change = false;
    lock.writeLock().lock();
    try{
      if (databases.containsKey(name)){
        Database db = databases.get(name);
        // TODO: db ...
        databases.remove(name);
        change = true;
        System.out.println("[DEBUG] " + "delete db "+name);
      }
      else {

        System.out.println("[DEBUG] " + "non-existed db "+name);
        throw new KeyNotExistException();
      }
    } finally {
      if (change) persistDatabases();
      lock.writeLock().unlock();
    }
  }

  public ArrayList<String> getDatabaseNames(){
    return new ArrayList<>(databases.keySet());
  }
  public void switchDatabase() {
    // TODO
  }

  public void persistDatabases() {
    // without lock
    try {
      FileOutputStream fos = new FileOutputStream(MANAGER_DATAPATH);
      OutputStreamWriter writer = new OutputStreamWriter(fos);
      for (String databaseName : databases.keySet())
        writer.write(databaseName + "\n");
      writer.close();
      fos.close();
    } catch (Exception e){
      e.printStackTrace();
      // throw exception
    }

  }

  private void recover() {
    File readDatabasesFile = new File(MANAGER_DATAPATH);
    if (!readDatabasesFile.exists()) return;
    try {
      FileReader fileReader = new FileReader(MANAGER_DATAPATH);
      BufferedReader reader = new BufferedReader(fileReader);
      String line;
      while ((line = reader.readLine()) != null) {
        createDatabaseIfNotExists(line);
      }
    } catch (Exception e){
      e.printStackTrace();
      // throw exception
    }
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();

    private ManagerHolder() {}
  }
}

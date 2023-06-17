package cn.edu.thssdb.utils;

import cn.edu.thssdb.schema.Row;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Logger {
  private String folder_name;
  private String file_name;
  private String full_path;

  // write lock
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public Logger(String folder_name, String file_name) {
    this.folder_name = folder_name;
    this.file_name = file_name;
    this.full_path = Paths.get(folder_name, file_name).toString();

    File d = new File(this.folder_name);
    if (!d.isDirectory()) {
      d.mkdirs();
    }
    File f = new File(this.full_path);
    if (!f.isFile()) {
      try {
        f.createNewFile();
      } catch (IOException e) {
        throw new RuntimeException();
      }
    }
  }

  public ArrayList<String> readLog() {
    ArrayList<String> lines = new ArrayList<>();
    String str;
    try {
      lock.readLock().lock();
      BufferedReader reader = new BufferedReader(new FileReader(full_path));
      while ((str = reader.readLine()) != null) {
        lines.add(str);
      }
      reader.close();
    } catch (IOException e) {
      throw new RuntimeException();
    } finally {
      lock.readLock().unlock();
    }
    return lines;
  }

  public void resetFile(List<String> lines) {
    try {
      lock.writeLock().lock();
      File f = new File(full_path);
      f.delete();
      f.createNewFile();
      writeLines(lines);
    } catch (IOException e) {
      throw new RuntimeException();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void writeLines(List<String> lines) {
    try {
      lock.writeLock().lock();
      BufferedWriter writer = new BufferedWriter(new FileWriter(full_path, true));
      for (String line : lines) {
        writer.write(line);
        writer.newLine();
      }
      writer.close();
    } catch (IOException e) {
      throw new RuntimeException();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void message(String m) {
    String log = "MESSAGE@" + m;
    List<String> Array = new ArrayList<>();
    Array.add(log);
    writeLines(Array);
  }

  public void delete(String database, String table, Row r) {
    String log = "DELETE@" + database + "@" + table + "@" + r.toString();
    List<String> Array = new ArrayList<>();
    Array.add(log);
    writeLines(Array);
  }

  public void insert(String database, String table, Row r) {
    String log = "INSERT@" + database + "@" + table + "@" + r.toString();
    List<String> Array = new ArrayList<>();
    Array.add(log);
    writeLines(Array);
  }

  public void update(String database, String table, Row oldRow, Row newRow) {
    delete(database, table, oldRow);
    insert(database, table, newRow);
  }

  public void dropDatabase(String database) {
    List<String> lines = readLog();
    for (int i = 0; i < lines.size(); i++) {
      String[] log = lines.get(i).split("@");
      if (log[0].equals("CREATE") && log[1].equals(database)) {
        lines.remove(i);
        i--;
      } else if (log[0].equals("INSERT") && log[1].equals(database)) {
        lines.remove(i);
        i--;
      } else if (log[0].equals("DELETE") && log[1].equals(database)) {
        lines.remove(i);
        i--;
      } else if (log[0].equals("UPDATE") && log[1].equals(database)) {
        lines.remove(i);
        i--;
      } else if (log[0].equals("DROP") && log[1].equals(database)) {
        lines.remove(i);
        i--;
      }
    }
    resetFile(lines);
  }

  public void dropTable(String database, String table) {
    List<String> lines = readLog();
    for (int i = 0; i < lines.size(); i++) {
      String[] log = lines.get(i).split("@");
      if (log[0].equals("CREATE") && log[1].equals(database) && log[2].equals(table)) {
        lines.remove(i);
        i--;
      } else if (log[0].equals("INSERT") && log[1].equals(database) && log[2].equals(table)) {
        lines.remove(i);
        i--;
      } else if (log[0].equals("DELETE") && log[1].equals(database) && log[2].equals(table)) {
        lines.remove(i);
        i--;
      } else if (log[0].equals("UPDATE") && log[1].equals(database) && log[2].equals(table)) {
        lines.remove(i);
        i--;
      } else if (log[0].equals("DROP") && log[1].equals(database) && log[2].equals(table)) {
        lines.remove(i);
        i--;
      }
    }
    resetFile(lines);
  }

  public void checkPoint() {
    List<String> lines = new ArrayList<>();
    String log = "CHECKPOINT";
    lines.add(log);
    resetFile(lines);
  }

  public void deleteFile() {
    File f = new File(this.full_path);
    f.delete();
  }
}

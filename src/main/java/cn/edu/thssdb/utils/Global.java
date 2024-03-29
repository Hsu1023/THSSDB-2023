package cn.edu.thssdb.utils;

public class Global {
  public static int fanout = 129;

  public static int SUCCESS_CODE = 0;
  public static int FAILURE_CODE = -1;

  public static String DEFAULT_SERVER_HOST = "127.0.0.1";
  public static int DEFAULT_SERVER_PORT = 6667;
  public static String DEFAULT_USER_NAME = "root";
  public static String DEFAULT_PASSWORD = "root";
  public static String CLI_PREFIX = "ThssDB2023>";
  public static final String SHOW_TIME = "show time;";
  public static final String CONNECT = "connect";
  public static final String DISCONNECT = "disconnect;";
  public static final String QUIT = "quit;";

  public static final String S_URL_INTERNAL = "jdbc:default:connection";

  public static final String DATA_PATH = "./data";

  public static final int PAGE_SIZE = 8192 * 2;

  public static final int INIT_PAGE_NUM = 4;

  public static final String LOG_PATH = "./log";

  public static final int ARRAY_LIST_MAX_LENGTH = (int) (3 * Global.fanout) + 1;

  public static final int BUFFER_POOL_SIZE = 5000;

  public static final String SEPERATE_LEVEL =
      "SERIALIZABLE"; // could be changed into "READ_COMMITTED" or "SERIALIZABLE"

  public static final Boolean RECOVER_FROM_DISC = false;
}

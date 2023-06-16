package cn.edu.thssdb.service;

import cn.edu.thssdb.plan.LogicalGenerator;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.impl.*;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.rpc.thrift.ConnectReq;
import cn.edu.thssdb.rpc.thrift.ConnectResp;
import cn.edu.thssdb.rpc.thrift.DisconnectReq;
import cn.edu.thssdb.rpc.thrift.DisconnectResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.rpc.thrift.GetTimeResp;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.StatusUtil;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static cn.edu.thssdb.plan.LogicalPlan.LogicalPlanType.*;
import static cn.edu.thssdb.plan.LogicalPlan.LogicalPlanType.UPDATE;

public class IServiceHandler implements IService.Iface {

  private static final AtomicInteger sessionCnt = new AtomicInteger(0);

  @Override
  public GetTimeResp getTime(GetTimeReq req) throws TException {
    GetTimeResp resp = new GetTimeResp();
    resp.setTime(new Date().toString());
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public ConnectResp connect(ConnectReq req) throws TException {
    return new ConnectResp(StatusUtil.success(), sessionCnt.getAndIncrement());
  }

  @Override
  public DisconnectResp disconnect(DisconnectReq req) throws TException {
    Manager manager = Manager.getInstance();
    //    manager.quit();
    return new DisconnectResp(StatusUtil.success());
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    if (req.getSessionId() < 0) {
      return new ExecuteStatementResp(
          StatusUtil.fail("You are not connected. Please connect first."), false);
    }
    // TODO: implement execution logic
    //    System.out.println(req.statement);
    LogicalPlan plan = LogicalGenerator.generate(req.statement);
    Manager manager = Manager.getInstance();

    try {

      LogicalPlan.LogicalPlanType type = plan.getType();
      Boolean autocommit = false;
//      if (type == SELECT || type == DELETE || type == INSERT || type == UPDATE) {
//        synchronized (manager.meta_lock) {
//          if (!manager.transaction_list.contains(req.sessionId)) {
//            autocommit = true;
//          }
//        }
//      }
      switch (plan.getType()) {
        case CREATE_DB:
          //          System.out.println("[DEBUG] " + plan + " Session ID:" +
          // String.valueOf(req.sessionId));
          CreateDatabasePlan createDBPlan = (CreateDatabasePlan) plan;
          manager.createDatabaseIfNotExists(createDBPlan.getDatabaseName());
          return new ExecuteStatementResp(StatusUtil.success(), false);

        case SHOW_DB:
          //          System.out.println("[DEBUG] " + plan + " Session ID:" +
          // String.valueOf(req.sessionId));
          ExecuteStatementResp resp_showdb = new ExecuteStatementResp(StatusUtil.success(), true);
          resp_showdb.setColumnsList(Arrays.asList("Names of Databases"));
          List<String> names = manager.getDatabaseNames();
          List<List<String>> result = new ArrayList<>();
          for (String name : names) result.add(Arrays.asList(name));
          resp_showdb.setRowList(result);
          return resp_showdb;

        case DROP_DB:
          //          System.out.println("[DEBUG] " + plan + " Session ID:" +
          // String.valueOf(req.sessionId));
          DropDatabasePlan dropDBPlan = (DropDatabasePlan) plan;
          String dbName = dropDBPlan.getDatabaseName();
          manager.deleteDatabase(dbName);
          return new ExecuteStatementResp(StatusUtil.success(), false);

        case USE_DB:
          //          System.out.println("[DEBUG] " + plan + " Session ID:" +
          // String.valueOf(req.sessionId));
          UseDatabasePlan useDBPlan = (UseDatabasePlan) plan;
          String currDB = useDBPlan.getDatabaseName();
          manager.switchDatabase(currDB);
          return new ExecuteStatementResp(StatusUtil.success(), false);

        case CREATE_TB:
          //          System.out.println("[DEBUG] " + plan + " Session ID:" +
          // String.valueOf(req.sessionId));
          CreateTablePlan createTBPlan = (CreateTablePlan) plan;
          manager.createTableIfNotExist(createTBPlan.getCtx());
          return new ExecuteStatementResp(StatusUtil.success(), false);

        case SHOW_TB:
          //          System.out.println("[DEBUG] " + plan + " Session ID:" +
          // String.valueOf(req.sessionId));
          ShowTablePlan showTBPlan = (ShowTablePlan) plan;
          String tableMessage = manager.showTable(showTBPlan.getTableName());
          ExecuteStatementResp resp_showtb = new ExecuteStatementResp(StatusUtil.success(), true);
          resp_showtb.setColumnsList(Arrays.asList("Table Content"));
          List<List<String>> res = new ArrayList<>();
          res.add(Arrays.asList(tableMessage));
          resp_showtb.setRowList(res);
          return resp_showtb;

        case DROP_TB:
          //          System.out.println("[DEBUG] " + plan + " Session ID:" +
          // String.valueOf(req.sessionId));
          DropTablePlan dropTBPlan = (DropTablePlan) plan;
          String tbName = dropTBPlan.getTableName();
          manager.deleteTable(tbName);
          return new ExecuteStatementResp(StatusUtil.success(), false);

        case INSERT:
          if (autocommit) {
            manager.beginTransaction(req.sessionId);
          }
          //          System.out.println("[DEBUG] " + plan + " Session ID:" +
          // String.valueOf(req.sessionId));
          InsertPlan insertPlan = (InsertPlan) plan;
          manager.insert(insertPlan.getCtx(), req.getSessionId());
          if (autocommit) {
            manager.commit(req.sessionId);
          }
          return new ExecuteStatementResp(StatusUtil.success(), false);

        case DELETE:
          if (autocommit) {
//            Manager.logger.session("[DEBUG] " + "AUTO COMMIT DELETE" + " Session ID:" + req.sessionId);
            manager.beginTransaction(req.sessionId);
          }
          //          System.out.println("[DEBUG] " + plan + " Session ID:" +
          // String.valueOf(req.sessionId));
          DeletePlan deletePlan = (DeletePlan) plan;
          manager.delete(deletePlan.getCtx(), req.getSessionId());
          if (autocommit) {
            manager.commit(req.sessionId);
          }
          return new ExecuteStatementResp(StatusUtil.success(), false);
        case SELECT:
          if (autocommit) {
            manager.beginTransaction(req.sessionId);
          }
          //          System.out.println("[DEBUG] " + plan + " Session ID:" +
          // String.valueOf(req.sessionId));
          SelectPlan selectPlan = (SelectPlan) plan;
          QueryResult queryResult = manager.select(selectPlan.getCtx(), req.getSessionId());
          ExecuteStatementResp resp = new ExecuteStatementResp(StatusUtil.success(), true);
          for (Row row : queryResult.results) resp.addToRowList(row.toStringList());
          if (queryResult.results.size() == 0) {
            List<List<String>> empty_list = new ArrayList<>();
            resp.setRowList(empty_list);
          }
          for (String columnName : queryResult.getColumnNames()) resp.addToColumnsList(columnName);
          if (autocommit) {
            manager.commit(req.sessionId);
          }

          return resp;

        case UPDATE:
          if (autocommit) {
            manager.beginTransaction(req.sessionId);
          }
          //          System.out.println("[DEBUG] " + plan + " Session ID:" +
          // String.valueOf(req.sessionId));
          UpdatePlan updatePlan = (UpdatePlan) plan;
          manager.update(updatePlan.getCtx(), req.getSessionId());
          if (autocommit) {
            manager.commit(req.sessionId);
          }
          return new ExecuteStatementResp(StatusUtil.success(), false);

        case BEGIN_TRANS:
          //          System.out.println("[DEBUG] " + plan + " Session ID:" +
          // String.valueOf(req.sessionId));
          BeginTransactionPlan beginTransactionPlan = (BeginTransactionPlan) plan;
          manager.beginTransaction(req.getSessionId());
          return new ExecuteStatementResp(StatusUtil.success(), false);

        case COMMIT:
          //          System.out.println("[DEBUG] " + plan + " Session ID:" +
          // String.valueOf(req.sessionId));
          CommitPlan commitPlan = (CommitPlan) plan;
          manager.commit(req.getSessionId());
          return new ExecuteStatementResp(StatusUtil.success(), false);
        case CHECKPOINT:
          //          System.out.println("[DEBUG] " + plan + " Session ID:" +
          // String.valueOf(req.sessionId));
          manager.checkPoint();
          return new ExecuteStatementResp(StatusUtil.success(), false);
        default:
      }
    } catch (Exception e) {
      e.printStackTrace();
      return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
    }
    return null;
  }
}

package cn.edu.thssdb.service;

import cn.edu.thssdb.plan.LogicalGenerator;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.impl.*;
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
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.StatusUtil;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

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
    return new DisconnectResp(StatusUtil.success());
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    if (req.getSessionId() < 0) {
      return new ExecuteStatementResp(
          StatusUtil.fail("You are not connected. Please connect first."), false);
    }
    // TODO: implement execution logic
    LogicalPlan plan = LogicalGenerator.generate(req.statement);
    Manager manager = Manager.getInstance();
    switch (plan.getType()) {
      case CREATE_DB:
        System.out.println("[DEBUG] " + plan);
        CreateDatabasePlan createDBPlan = (CreateDatabasePlan) plan;
        manager.createDatabaseIfNotExists(createDBPlan.getDatabaseName());
        return new ExecuteStatementResp(StatusUtil.success(), false);

      case SHOW_DB:
        System.out.println("[DEBUG] " + plan);
        ExecuteStatementResp resp_showdb = new ExecuteStatementResp(StatusUtil.success(), true);
        resp_showdb.setColumnsList(Arrays.asList("Names of Databases"));
        List<String> names = manager.getDatabaseNames();
        List<List<String>> result = new ArrayList<>();
        for (String name : names) result.add(Arrays.asList(name));
        resp_showdb.setRowList(result);
        return resp_showdb;

      case DROP_DB:
        System.out.println("[DEBUG] " + plan);
        DropDatabasePlan dropDBPlan = (DropDatabasePlan) plan;
        String dbName = dropDBPlan.getDatabaseName();
        manager.deleteDatabase(dbName);
        return new ExecuteStatementResp(StatusUtil.success(), false);

      case USE_DB:
        System.out.println("[DEBUG] " + plan);
        UseDatabasePlan useDBPlan = (UseDatabasePlan) plan;
        String currDB = useDBPlan.getDatabaseName();
        manager.switchDatabase(currDB);
        return new ExecuteStatementResp(StatusUtil.success(), false);

      case CREATE_TB:
        System.out.println("[DEBUG] " + plan);
        CreateTablePlan createTBPlan = (CreateTablePlan) plan;
        manager.createTableIfNotExist(createTBPlan.getCtx());
        return new ExecuteStatementResp(StatusUtil.success(), false);

      case SHOW_TB:
        System.out.println("[DEBUG] " + plan);
        ShowTablePlan showTBPlan = (ShowTablePlan) plan;
        String tableMessage = manager.showTable(showTBPlan.getTableName());
        ExecuteStatementResp resp_showtb = new ExecuteStatementResp(StatusUtil.success(), true);
        resp_showtb.setColumnsList(Arrays.asList("Table Content"));
        List<List<String>> res = new ArrayList<>();
        res.add(Arrays.asList(tableMessage));
        resp_showtb.setRowList(res);
        return resp_showtb;

      case DROP_TB:
        System.out.println("[DEBUG] " + plan);
        DropTablePlan dropTBPlan = (DropTablePlan) plan;
        String tbName = dropTBPlan.getTableName();
        manager.deleteTable(tbName);
        return new ExecuteStatementResp(StatusUtil.success(), false);

      case INSERT:
        System.out.println("[DEBUG] " + plan);
        InsertPlan insertPlan = (InsertPlan) plan;
        manager.insert(insertPlan.getCtx());
        return new ExecuteStatementResp(StatusUtil.success(), false);
      default:
    }
    return null;
  }
}

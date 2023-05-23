package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.sql.SQLParser;

public class SelectPlan extends LogicalPlan {
  private SQLParser.SelectStmtContext ctx;

  public SelectPlan(SQLParser.SelectStmtContext ctx) {
    super(LogicalPlanType.SELECT);
    this.ctx = ctx;
  }

  public SQLParser.SelectStmtContext getCtx() {
    return ctx;
  }

  @Override
  public String toString() {
    String tableName = ctx.tableQuery().get(0).getText();
    return "SelectPlan{" + "firstTableName='" + tableName + '\'' + '}';
  }
}

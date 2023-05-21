package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.sql.SQLParser;

public class InsertPlan extends LogicalPlan {
  private SQLParser.InsertStmtContext ctx;

  public InsertPlan(SQLParser.InsertStmtContext ctx) {
    super(LogicalPlan.LogicalPlanType.INSERT);
    this.ctx = ctx;
  }

  public SQLParser.InsertStmtContext getCtx() {
    return ctx;
  }

  @Override
  public String toString() {
    return "InsertPlan{" + "tableName='" + ctx.tableName().children.get(0).toString() + '\'' + '}';
  }
}

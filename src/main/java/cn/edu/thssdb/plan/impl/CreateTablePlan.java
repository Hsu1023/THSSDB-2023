package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.sql.SQLParser;

public class CreateTablePlan extends LogicalPlan {
  private SQLParser.CreateTableStmtContext ctx;

  public CreateTablePlan(SQLParser.CreateTableStmtContext ctx) {
    super(LogicalPlanType.CREATE_TB);
    this.ctx = ctx;
  }

  public SQLParser.CreateTableStmtContext getCtx() {
    return ctx;
  }

  @Override
  public String toString() {
    return "CreateTablePlan{"
        + "tableName='"
        + ctx.tableName().children.get(0).toString()
        + '\''
        + '}';
  }
}

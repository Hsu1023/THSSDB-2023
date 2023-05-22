package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.sql.SQLParser;

public class DeletePlan extends LogicalPlan {
  private SQLParser.DeleteStmtContext ctx;

  public DeletePlan(SQLParser.DeleteStmtContext ctx) {
    super(LogicalPlan.LogicalPlanType.DELETE);
    this.ctx = ctx;
  }

  public SQLParser.DeleteStmtContext getCtx() {
    return ctx;
  }

  @Override
  public String toString() {
    return "DeletePlan{" + "tableName='" + ctx.tableName().children.get(0).toString() + '\'' + '}';
  }
}

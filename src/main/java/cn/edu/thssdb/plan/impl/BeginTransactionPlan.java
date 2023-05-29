package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.sql.SQLParser;

public class BeginTransactionPlan extends LogicalPlan {
  private SQLParser.BeginTransactionStmtContext ctx;

  public BeginTransactionPlan(SQLParser.BeginTransactionStmtContext ctx) {
    super(LogicalPlanType.BEGIN_TRANS);
    this.ctx = ctx;
  }

  public SQLParser.BeginTransactionStmtContext getCtx() {
    return ctx;
  }

  @Override
  public String toString() {
    return "BeginTransactionPlan";
  }
}

package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.sql.SQLParser;

public class CommitPlan extends LogicalPlan {
  private SQLParser.CommitStmtContext ctx;

  public CommitPlan(SQLParser.CommitStmtContext ctx) {
    super(LogicalPlan.LogicalPlanType.COMMIT);
    this.ctx = ctx;
  }

  public SQLParser.CommitStmtContext getCtx() {
    return ctx;
  }

  @Override
  public String toString() {
    return "CommitPlan";
  }
}

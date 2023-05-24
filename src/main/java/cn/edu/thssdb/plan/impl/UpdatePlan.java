package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.sql.SQLParser;

public class UpdatePlan extends LogicalPlan {
  private SQLParser.UpdateStmtContext ctx;

  public UpdatePlan(SQLParser.UpdateStmtContext ctx) {
    super(LogicalPlanType.UPDATE);
    this.ctx = ctx;
  }

  public SQLParser.UpdateStmtContext getCtx() {
    return ctx;
  }

  //    @Override
  //    public String toString() {
  //        return "UpdatePlan{" + "tableName='" + ctx.tableName().children.get(0).toString() + '\''
  // + '}';
  //    }
}

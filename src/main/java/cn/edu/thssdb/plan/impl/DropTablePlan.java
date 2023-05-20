package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

public class DropTablePlan extends LogicalPlan {
  private String tableName;

  public DropTablePlan(String tableName) {
    super(LogicalPlanType.DROP_TB);
    this.tableName = tableName.toLowerCase();
  }

  public String getTableName() {
    return tableName;
  }

  @Override
  public String toString() {
    return "DropTablePlan: database " + tableName;
  }
}

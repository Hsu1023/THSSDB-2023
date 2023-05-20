package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

public class UseDatabasePlan extends LogicalPlan {
  private String databaseName;

  public UseDatabasePlan(String databaseName) {
    super(LogicalPlanType.USE_DB);
    this.databaseName = databaseName.toLowerCase();
  }

  public String getDatabaseName() {
    return databaseName;
  }

  @Override
  public String toString() {
    return "UseDatabasesPlan: database " + databaseName;
  }
}

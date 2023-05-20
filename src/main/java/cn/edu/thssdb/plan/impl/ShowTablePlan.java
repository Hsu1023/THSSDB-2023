package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

public class ShowTablePlan extends LogicalPlan {
  private String name;

  public ShowTablePlan(String name) {
    super(LogicalPlanType.SHOW_TB);
    this.name = name;
  }

  public String getTableName() {
    return name;
  }

  @Override
  public String toString() {
    return "ShowTablePlan";
  }
}

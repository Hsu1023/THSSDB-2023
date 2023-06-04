package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

public class CheckpointPlan extends LogicalPlan {
  public CheckpointPlan() {
    super(LogicalPlanType.CHECKPOINT);
  }

  @Override
  public String toString() {
    return "CheckpointPlan";
  }
}

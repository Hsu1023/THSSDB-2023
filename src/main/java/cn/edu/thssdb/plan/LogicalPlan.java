package cn.edu.thssdb.plan;

public abstract class LogicalPlan {

  protected LogicalPlanType type;

  public LogicalPlan(LogicalPlanType type) {
    this.type = type;
  }

  public LogicalPlanType getType() {
    return type;
  }

  public enum LogicalPlanType {
    // TODO: add more LogicalPlanType
    CREATE_DB,
    SHOW_DB,
    DROP_DB,
    USE_DB,
    CREATE_TB,
    SHOW_TB,
    DROP_TB,
    INSERT,
    DELETE,
    UPDATE,
    SELECT,
    BEGIN_TRANS,
    COMMIT,
    CHECKPOINT,
  }
}

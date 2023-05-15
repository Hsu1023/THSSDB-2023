package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

public class DropDatabasePlan extends LogicalPlan {

    private String databaseName;
    public DropDatabasePlan(String databaseName) {
        super(LogicalPlanType.DROP_DB);
        this.databaseName = databaseName.toLowerCase();
    }

    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String toString() {
        return "DropDatabasesPlan: database " + databaseName;
    }
}
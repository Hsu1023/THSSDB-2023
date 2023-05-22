/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.thssdb.parser;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.impl.*;
import cn.edu.thssdb.sql.SQLBaseVisitor;
import cn.edu.thssdb.sql.SQLParser;

public class ThssDBSQLVisitor extends SQLBaseVisitor<LogicalPlan> {

  @Override
  public LogicalPlan visitCreateDbStmt(SQLParser.CreateDbStmtContext ctx) {
    return new CreateDatabasePlan(ctx.databaseName().getText());
  }

  @Override
  public LogicalPlan visitShowDbStmt(SQLParser.ShowDbStmtContext ctx) {
    return new ShowDatabasePlan();
  }

  @Override
  public LogicalPlan visitDropDbStmt(SQLParser.DropDbStmtContext ctx) {
    return new DropDatabasePlan(ctx.databaseName().getText());
  }

  @Override
  public LogicalPlan visitUseDbStmt(SQLParser.UseDbStmtContext ctx) {
    return new UseDatabasePlan(ctx.databaseName().getText());
  }

  @Override
  public LogicalPlan visitCreateTableStmt(SQLParser.CreateTableStmtContext ctx) {
    return new CreateTablePlan(ctx);
  }

  @Override
  public LogicalPlan visitShowTableStmt(SQLParser.ShowTableStmtContext ctx) {
    return new ShowTablePlan(ctx.tableName().children.get(0).toString());
  }

  @Override
  public LogicalPlan visitDropTableStmt(SQLParser.DropTableStmtContext ctx) {
    return new DropTablePlan(ctx.tableName().children.get(0).toString());
  }

  @Override
  public LogicalPlan visitInsertStmt(SQLParser.InsertStmtContext ctx) {
    return new InsertPlan(ctx);
  }
  // TODO: parser to more logical plan

  @Override
  public LogicalPlan visitDeleteStmt(SQLParser.DeleteStmtContext ctx) {
    return new DeletePlan(ctx);
  }
}

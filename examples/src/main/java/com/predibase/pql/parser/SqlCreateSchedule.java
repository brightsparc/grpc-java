/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.predibase.pql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.util.*;
import org.checkerframework.dataflow.qual.*;

import java.util.*;

/**
 * A <code>SqlCreateSchedule</code> is a node of a parse tree which creates
 * a schedule with start date and interval for a query.
 *
 */
public class SqlCreateSchedule extends SqlCreate {
  //~ Enums ------------------------------------------------------------------

  /**
   * The prediction type (either predict or evaluate).
   */
  public enum ScheduleType implements Symbolizable {
    /**
     * The prediction type is a string in a CRON format.
     */
    CRON,
    /**
     * The prediction type is string in an INTERVAL format.
     */
    INTERVAL
  }

  //~ Instance fields --------------------------------------------------------

  public final SqlIdentifier name;
  public final SqlLiteral start;
  public final ScheduleType scheduleType;
  public final SqlNode interval;
  public final SqlNode query;

  //~ Static Fields -----------------------------------------------------------

  public static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE SCHEDULE", SqlKind.OTHER_DDL);

  /** Creates a SqlCreateSchedule. */
  public SqlCreateSchedule(SqlParserPos pos, boolean replace, boolean ifNotExists,
      SqlIdentifier name, SqlLiteral start,
      ScheduleType scheduleType, SqlNode interval, SqlNode query) {
    super(OPERATOR, pos, replace, ifNotExists);
    this.name = Objects.requireNonNull(name, "name");
    this.start = Objects.requireNonNull(start, "start");
    this.scheduleType = Objects.requireNonNull(scheduleType, "scheduleType");
    this.interval = Objects.requireNonNull(interval, "interval");
    this.query = Objects.requireNonNull(query, "query");
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name,
        scheduleType.symbol(SqlParserPos.ZERO), start, interval, query);
  }

  @Pure
  public final SqlIdentifier getName() {
    return name;
  }

  @Pure
  public final SqlLiteral getStart() {
    return start;
  }

  @Pure
  public final ScheduleType getScheduleType() {
    return scheduleType;
  }

  @Pure
  public final SqlNode getInterval() {
    return interval;
  }

  @Pure
  public final SqlNode getQuery() {
    return query;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    if (getReplace()) {
      writer.keyword("OR REPLACE");
    }
    writer.keyword("SCHEDULE");
    name.unparse(writer, leftPrec, rightPrec);
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    writer.keyword("START");
    start.unparse(writer, leftPrec, rightPrec);
    // Includes CRON or INTERVAL keyword prefix
    interval.unparse(writer, leftPrec, rightPrec);
    writer.keyword("AS");
    writer.newlineAndIndent();
    query.unparse(writer, leftPrec, rightPrec);
  }
}

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
 * A <code>SqlCreateDataset</code> is a node of a parse tree {@code CREATE DATASET}
 * which creates a dataset which can either be a direct mapping from a connection eg:
 *
 * <code>CREATE DATASET dataset_name FROM db_connection_name.table_name
 * </code>
 *
 * Or can be a materialized view eg:
 *
 * <code>CREATE DATASET dataset_name AS
 *   SELECT * FROM db_connection_name.table_name
 *   WHERE date_col < now()
 * </code>
 *
 * In the case of an object store connection you can optionally provide hits for s3 prefix
 *
 * <code>CREATE DATASET dataset_name AS
 *   SELECT * FROM s3_connection_name
 *   WHERE date_col < now()
 * </code>
 *
 * On each case the <code>FROM</code> clause references a connection and table.
 */
public class SqlCreateDataset extends SqlCreate {
  //~ Instance fields --------------------------------------------------------

  public final SqlIdentifier name;
  public final SqlDatasetRef targetRef;
  public final SqlDatasetRef sourceRef;
  public final SqlNode query;

  //~ Static Fields -----------------------------------------------------------

  public static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE DATASET", SqlKind.OTHER_DDL);

  /** Creates a SqlCreateSchedule. */
  public SqlCreateDataset(SqlParserPos pos, boolean replace, boolean ifNotExists,
                          SqlIdentifier name, SqlDatasetRef targetRef, SqlDatasetRef sourceRef, SqlNode query) {
    super(OPERATOR, pos, replace, ifNotExists);
    this.name = Objects.requireNonNull(name, "name");
    this.targetRef = targetRef;
    this.sourceRef = sourceRef;
    this.query = query;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, targetRef, sourceRef, query);
  }

  @Pure
  public final SqlIdentifier getName() {
    return name;
  }

  /** Returns the given name as a type. */
  public <T extends Object> T getNameAs(Class<T> clazz) {
    if (clazz.isInstance(name)) {
      return clazz.cast(name);
    }
    // If we are asking for a string, get the simple name, or use
    if (clazz == String.class) {
      if (name.isSimple()) {
        return clazz.cast(name.getSimple());
      }
      return clazz.cast(name.toString());
    }
    throw new AssertionError("cannot cast " + name + " as " + clazz);
  }

  @Pure
  public final SqlDatasetRef getTargetRef() {
    return targetRef;
  }

  @Pure
  public final SqlDatasetRef getSourceRef() {
    return sourceRef;
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
    writer.keyword("DATASET");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);
    // Optional into command
    if (targetRef != null) {
      writer.newlineAndIndent();
      writer.keyword("INTO");
      targetRef.unparse(writer, leftPrec, rightPrec);
    }
    // From source table, or query
    if (sourceRef != null) {
      writer.newlineAndIndent();
      writer.keyword("FROM");
      sourceRef.unparse(writer, leftPrec, rightPrec);
    } else if (query != null) {
      writer.keyword("AS");
      writer.newlineAndIndent();
      query.unparse(writer, leftPrec, rightPrec);
    }
  }
}

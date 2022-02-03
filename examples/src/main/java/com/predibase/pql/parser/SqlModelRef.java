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

import com.google.common.collect.*;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.*;
import org.checkerframework.checker.nullness.qual.*;

import java.util.*;

/**
 * A <code>SqlModelRef</code> is a node of a parse tree which represents
 * a sql given statement for the <code>SqlCreateModel</code> clause.
 *
 * <p>Basic grammar is the tableRef followed by optional uri and format.
 **/
public class SqlModelRef extends SqlCall {
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("DATASET REF", SqlKind.OTHER) {
        @Override public SqlCall createCall(
            @Nullable SqlLiteral functionQualifier,
            SqlParserPos pos,
            @Nullable SqlNode... operands) {
          return new SqlModelRef(pos,
              (SqlIdentifier) Objects.requireNonNull(operands[0], "name"),
              ((SqlLiteral) Objects.requireNonNull(operands[2], "version"))
                  .getValueAs(Integer.class));
        }
      };

  //~ Instance fields --------------------------------------------------------

  private final SqlIdentifier name;
  private final int version;

  //~ Constructors -----------------------------------------------------------

  public SqlModelRef(SqlParserPos pos, SqlIdentifier name, int version) {
    super(pos);
    this.name = Objects.requireNonNull(name, "name");
    this.version = version;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(name,
        SqlNumericLiteral.createExactNumeric(Integer.toString(version), SqlParserPos.ZERO));
  }

  /** Returns the dataset name as a sql identifier. */
  public SqlIdentifier getName() {
    return name;
  }

  /** Returns the dataset uri as a sql literal. */
  public int getVersion() {
    return version;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    name.unparse(writer, 0, 0);
    if (version != RelDataType.PRECISION_NOT_SPECIFIED) {
      writer.keyword("VERSION");
      writer.print(version);
    }
  }
}

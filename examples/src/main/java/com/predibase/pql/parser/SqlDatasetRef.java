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
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.*;
import org.checkerframework.checker.nullness.qual.*;

import java.util.*;

/**
 * A <code>SqlDatasetRef</code> is a node of a parse tree which represents
 * a sql given statement for the <code>SqlCreateDataset</code> clause.
 *
 * <p>Basic grammar is the tableRef followed by optional uri and format.
 **/
public class SqlDatasetRef extends SqlCall {
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("DATASET REF", SqlKind.OTHER) {
        @Override public SqlCall createCall(
            @Nullable SqlLiteral functionQualifier,
            SqlParserPos pos,
            @Nullable SqlNode... operands) {
          return new SqlDatasetRef(pos, (SqlIdentifier) operands[0], operands[1], operands[2]);
        }
      };

  //~ Instance fields --------------------------------------------------------

  private final SqlIdentifier tableRef;
  private final SqlNode uri;
  private final SqlNode format;

  //~ Constructors -----------------------------------------------------------

  public SqlDatasetRef(SqlParserPos pos, SqlIdentifier tableRef, SqlNode uri, SqlNode format) {
    super(pos);
    this.tableRef = Objects.requireNonNull(tableRef, "tableRef");
    this.uri = uri;
    this.format = format;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(tableRef, uri, format);
  }

  /** Returns the dataset name as a sql identifier. */
  public SqlIdentifier getTableRef() {
    return tableRef;
  }

  /** Returns the dataset uri as a sql literal. */
  public SqlLiteral getUri() {
    return (SqlLiteral) uri;
  }

  /** Returns the dataset format. */
  public SqlNode getFormat() {
    return format;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    // TODO: Consider if we want to output DATASET before dataset names?
    // writer.keyword("DATASET");
    tableRef.unparse(writer, leftPrec, rightPrec);
    if (uri != null) {
      writer.keyword("DATASET_URI");
      writer.keyword("=");
      uri.unparse(writer, leftPrec, rightPrec);
    }
    if (format != null) {
      writer.keyword("DATASET_FORMAT");
      writer.keyword("=");
      format.unparse(writer, leftPrec, rightPrec);
    }
  }
}

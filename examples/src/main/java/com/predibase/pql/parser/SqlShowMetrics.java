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
import java.util.stream.*;

/**
 * A <code>SqlShowMetrics</code> is a node of a parse tree which represents
 * the instruction to show metrics.
 */
public class SqlShowMetrics extends SqlShow {
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("SHOW METRICS", SqlKind.OTHER);


  //~ Instance fields --------------------------------------------------------

  public final SqlNodeList metricList;
  public final SqlNodeList targetList;
  public final SqlNodeList modelList;

  //~ Constructors -----------------------------------------------------------

  /** Creates a SqlMetrics. */
  public SqlShowMetrics(SqlParserPos pos,
      SqlNodeList metricList, SqlNodeList targetList, SqlNodeList modelList) {
    super(OPERATOR, pos);
    this.metricList = metricList;
    this.targetList = targetList;
    this.modelList = Objects.requireNonNull(modelList, "modelList");
  }

  //~ Methods ----------------------------------------------------------------

  @Pure
  public final SqlNodeList getMetricList() {
    return metricList;
  }

  @Pure
  public final SqlNodeList getTargetList() {
    return targetList;
  }


  /** Return the list of model refs in from list. */
  public List<SqlModelRef> getModelList() {
    return modelList.stream().filter(f -> f instanceof SqlModelRef)
        .map(f -> (SqlModelRef) f).collect(Collectors.toList());
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(metricList, targetList, modelList);
  }

  @Override public void unparseCall(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("SHOW");
    writer.keyword("METRICS");
    if (metricList != null) {
      if (metricList.size() == 1) {
        metricList.unparse(writer, leftPrec, rightPrec);
      } else {
        SqlWriter.Frame frame = writer.startList("(", ")");
        metricList.unparse(writer, leftPrec, rightPrec);
        writer.endList(frame);
      }
    }
    if (targetList != null) {
      writer.keyword("TARGET");
      if (targetList.size() == 1) {
        targetList.unparse(writer, leftPrec, rightPrec);
      } else {
        SqlWriter.Frame frame = writer.startList("(", ")");
        targetList.unparse(writer, leftPrec, rightPrec);
        writer.endList(frame);
      }
    }
    if (modelList.size() > 0) {
      writer.keyword("FROM");
      if (modelList.size() == 1) {
        modelList.unparse(writer, leftPrec, rightPrec);
      } else {
        SqlWriter.Frame frame = writer.startList("(", ")");
        modelList.unparse(writer, leftPrec, rightPrec);
        writer.endList(frame);
      }
    }
  }

}

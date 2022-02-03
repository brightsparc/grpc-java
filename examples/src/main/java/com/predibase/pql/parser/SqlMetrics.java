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
public class SqlMetrics extends SqlCall {
  //~ Enums ------------------------------------------------------------------

  /**
   * The prediction type (either predict or evaluate).
   */
  public enum MetricsType implements Symbolizable {
    /**
     * The show type is Show.
     */
    SHOW,
    /**
     * The show type is Plot.
     */
    PLOT
  }

  //~ Instance fields --------------------------------------------------------

  public final SqlNodeList metricList;
  public final SqlNodeList targetList;
  public final SqlNodeList fromList;
  public final MetricsType metricsType;

  //~ Constructors -----------------------------------------------------------

  /** Creates a SqlPredict. */
  public SqlMetrics(SqlParserPos pos,
                        MetricsType metricsType,
                        SqlNodeList metricList,
                        SqlNodeList targetList,
                        SqlNodeList fromList) {
    super(pos);
    this.metricsType = metricsType;
    this.metricList = Objects.requireNonNull(metricList, "metricList");
    this.targetList = targetList;
    this.fromList = Objects.requireNonNull(fromList, "fromList");
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlOperator getOperator() {
    return new SqlSpecialOperator(metricsType.toString(), SqlKind.OTHER);
  }

  @Override public SqlKind getKind() {
    return SqlKind.OTHER;
  }


  @Pure
  public final MetricsType getShowType() {
    return metricsType;
  }

  @Pure
  public final SqlNodeList getMetricList() {
    return metricList;
  }

  @Pure
  public final SqlNodeList getTargetList() {
    return targetList;
  }

  @Pure
  public final SqlNodeList getFromList() {
    return fromList;
  }

  /** Return the list of dataset refs in from list. */
  public List<SqlDatasetRef> getDatasetList() {
    return getFromList().stream().filter(f -> f instanceof SqlDatasetRef)
            .map(f-> (SqlDatasetRef) f).collect(Collectors.toList());
  }

  /** Return the list of model refs in from list. */
  public List<SqlModelRef> getModelList() {
    return getFromList().stream().filter(f -> f instanceof SqlModelRef)
            .map(f-> (SqlModelRef) f).collect(Collectors.toList());
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(metricsType.symbol(SqlParserPos.ZERO), metricList, fromList);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    // Output either SHOW or PLOT
    writer.keyword(metricsType.toString());
    writer.keyword("METRICS");
    if (metricList.size() == 1) {
      metricList.unparse(writer, leftPrec, rightPrec);
    } else {
      SqlWriter.Frame metricsFrame = writer.startList("(", ")");
      metricList.unparse(writer, leftPrec, rightPrec);
      writer.endList(metricsFrame);
    }
    if (targetList != null) {
      writer.keyword("TARGET");
      if (targetList.size() == 1) {
        targetList.unparse(writer, leftPrec, rightPrec);
      } else {
        SqlWriter.Frame targetFrame = writer.startList("(", ")");
        targetList.unparse(writer, leftPrec, rightPrec);
        writer.endList(targetFrame);
      }
    }
    if (getFromList().size() > 0) {
      writer.keyword("FROM");
      SqlWriter.Frame modelFrame = writer.startList("(", ")");
      // Write datasets first
      if (getDatasetList().size() > 0) {
        writer.keyword("DATASET");
        getDatasetList().forEach(ds -> ds.unparse(writer, leftPrec, rightPrec));
      }
      // Write models second
      if (getModelList().size() > 0) {
        writer.keyword("MODEL");
        getModelList().forEach(ds -> ds.unparse(writer, leftPrec, rightPrec));
      }
      writer.endList(modelFrame);
    }
  }

}

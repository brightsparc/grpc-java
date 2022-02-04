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

import static java.util.Objects.*;

/**
 * A <code>SqlRangeReal</code> is a node of a parse tree which represents
 * a sql given statement for the <code>SqlPredict</code> clause.
 *
 * <p>Basic given grammar is: <code>RANGE_REAL(min, max [, steps[, log | linear]])</code>.
 */
public class SqlRangeReal extends SqlCall {
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("RANGE_REAL", SqlKind.OTHER) {
        @Override public SqlCall createCall(
            @Nullable SqlLiteral functionQualifier,
            SqlParserPos pos,
            @Nullable SqlNode... operands) {
          return new SqlRangeReal(pos,
              (SqlNumericLiteral) requireNonNull(operands[0], "min"),
              (SqlNumericLiteral) requireNonNull(operands[1], "max"),
              (SqlNumericLiteral) operands[2],
              ((SqlLiteral) requireNonNull(operands[3], "scaleType"))
                  .getValueAs(ScaleType.class));
        }
      };


  //~ Enums ------------------------------------------------------------------

  /**
   * The scale type.
   */
  public enum ScaleType implements Symbolizable {
     /**
     * The scale is auto.
     */
    AUTO,
    /**
     * The scale is linear.
     */
    LINEAR,
    /**
     * The scale is log.
     */
    LOG
  }

  //~ Instance fields --------------------------------------------------------

  private final SqlNumericLiteral min;
  private final SqlNumericLiteral max;
  private final SqlNumericLiteral steps;
  private final ScaleType scaleType;

  //~ Constructors -----------------------------------------------------------

  public SqlRangeReal(
      SqlParserPos pos,
      SqlNumericLiteral min,
      SqlNumericLiteral max,
      SqlNumericLiteral steps,
      ScaleType scaleType) {
    super(pos);
    this.min   = Objects.requireNonNull(min, "min");
    this.max = Objects.requireNonNull(max, "max");
    this.steps = steps;
    this.scaleType = Objects.requireNonNull(scaleType, "scaleType");
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(min, max, steps, scaleType.symbol(SqlParserPos.ZERO));
  }

  /** Returns the min value. */
  public SqlNumericLiteral getMin() {
    return min;
  }

  /** Returns the max value. */
  public SqlNumericLiteral getMax() {
    return max;
  }

  /** Returns the max value. */
  public SqlNumericLiteral getSteps() {
    return steps;
  }

  /** Returns the given type. */
  public ScaleType getScaleType() {
    return scaleType;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.print("RANGE_REAL"); // print so no space before brackets
    SqlWriter.Frame frame = writer.startList("(", ")");
    writer.sep(",");
    min.unparse(writer, leftPrec, rightPrec);
    writer.sep(",");
    max.unparse(writer, leftPrec, rightPrec);
    if (steps != null) {
      writer.sep(",");
      steps.unparse(writer, leftPrec, rightPrec);
    }
    if (scaleType != ScaleType.AUTO) {
      writer.sep(",");
      writer.keyword(scaleType.toString());
    }
    writer.endList(frame);
  }
}

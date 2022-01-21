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
 * A <code>SqlNumericRange</code> is a node of a parse tree which represents
 * a sql given statement for the <code>SqlPredict</code> clause.
 *
 * <p>Basic given grammar is: sequence (min, max[, step]) scale = [auto, log, linear].
 */
public class SqlGivenRange extends SqlCall {
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("SEQUENCE", SqlKind.OTHER) {
        @Override public SqlCall createCall(
            @Nullable SqlLiteral functionQualifier,
            SqlParserPos pos,
            @Nullable SqlNode... operands) {
          return new SqlGivenRange(pos,
              (SqlNumericLiteral) requireNonNull(operands[0], "min"),
              (SqlNumericLiteral) requireNonNull(operands[1], "max"),
              (SqlNumericLiteral) operands[2],
              ((SqlLiteral) requireNonNull(operands[3], "rangeType"))
                  .getValueAs(RangeType.class));
        }
      };


  //~ Enums ------------------------------------------------------------------

  /**
   * The given type.
   */
  public enum RangeType implements Symbolizable {
    /** Enumeration that  implements Symbolizable {
     /**
     * The given value is a simple identifier.
     */
    AUTO,
    /**
     * The given value is a numeric literal.
     */
    LINEAR,
    /**
     * The given value is a string literal.
     */
    LOG
  }

  //~ Instance fields --------------------------------------------------------

  private final SqlNumericLiteral min;
  private final SqlNumericLiteral max;
  private final SqlNumericLiteral step;
  private final RangeType rangeType;

  //~ Constructors -----------------------------------------------------------

  public SqlGivenRange(
      SqlParserPos pos,
      SqlNumericLiteral min,
      SqlNumericLiteral max,
      SqlNumericLiteral step,
      RangeType rangeType) {
    super(pos);
    this.min   = Objects.requireNonNull(min, "min");
    this.max = Objects.requireNonNull(max, "max");
    this.step = step;
    this.rangeType = Objects.requireNonNull(rangeType, "rangeType");
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(min, max, step, rangeType.symbol(SqlParserPos.ZERO));
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
  public SqlNumericLiteral getStep() {
    return step;
  }

  /** Returns the given type. */
  public RangeType getRangeType() {
    return rangeType;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("GIVEN_RANGE");
    SqlWriter.Frame frame = writer.startList("(", ")");
    writer.sep(",");
    min.unparse(writer, leftPrec, rightPrec);
    writer.sep(",");
    max.unparse(writer, leftPrec, rightPrec);
    if (step != null) {
      writer.sep(",");
      step.unparse(writer, leftPrec, rightPrec);
    }
    writer.endList(frame);
    if (rangeType != RangeType.AUTO) {
      writer.keyword("SCALE");
      writer.keyword("=");
      writer.keyword(rangeType.toString());
    }
  }
}

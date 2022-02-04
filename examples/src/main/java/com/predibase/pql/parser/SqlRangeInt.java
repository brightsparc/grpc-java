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
 * A <code>SqlRangeInt</code> is a node of a parse tree which represents
 * a sql given statement for the <code>SqlPredict</code> clause.
 *
 * <p>Basic given grammar is: <code>RANGE_INT(min, max [, step])</code>.
 */
public class SqlRangeInt extends SqlCall {
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("RANGE_INT", SqlKind.OTHER) {
        @Override public SqlCall createCall(
            @Nullable SqlLiteral functionQualifier,
            SqlParserPos pos,
            @Nullable SqlNode... operands) {
          return new SqlRangeInt(pos,
              ((SqlLiteral) Objects.requireNonNull(operands[0], "min"))
                  .getValueAs(Integer.class),
              ((SqlLiteral) Objects.requireNonNull(operands[1], "max"))
                  .getValueAs(Integer.class),
              ((SqlLiteral) operands[2]).getValueAs(Integer.class));
        }
      };

  //~ Instance fields --------------------------------------------------------

  private final int min;
  private final int max;
  private final int step;

  //~ Constructors -----------------------------------------------------------

  public SqlRangeInt(SqlParserPos pos, Integer min, Integer max, Integer step) {
    super(pos);
    this.min   = Objects.requireNonNull(min, "min");
    this.max = Objects.requireNonNull(max, "max");
    this.step = step;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(
        SqlNumericLiteral.createExactNumeric(Integer.toString(min), SqlParserPos.ZERO),
        SqlNumericLiteral.createExactNumeric(Integer.toString(max), SqlParserPos.ZERO),
        SqlNumericLiteral.createExactNumeric(Integer.toString(step), SqlParserPos.ZERO));
  }

  /** Returns the min value. */
  public int getMin() {
    return min;
  }

  /** Returns the max value. */
  public int getMax() {
    return max;
  }

  /** Returns the max value. */
  public int getStep() {
    return step;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.print("RANGE_INT"); // print so no space before brackets
    SqlWriter.Frame frame = writer.startList("(", ")");
    writer.sep(",");
    writer.print(min);
    writer.sep(",");
    writer.print(max);
    if (step != RelDataType.PRECISION_NOT_SPECIFIED) {
      writer.sep(",");
      writer.print(step);
    }
    writer.endList(frame);
  }
}

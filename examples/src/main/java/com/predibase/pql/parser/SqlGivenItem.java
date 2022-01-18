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

import java.util.*;

import static java.util.Objects.*;

/**
 * A <code>SqlPredictGiven</code> is a node of a parse tree which represents
 * a sql given statement for the <code>SqlPredict</code> clause.
 *
 * <p>Basic given grammar is: given_name=given_value.
 * The given key relates to a field in the select clause.
 * The given value can be ome of the following types:
 *
 * <ul>
 *   <li>simple identifier</li>
 *   <li>integer literal</li>
 *   <li>string literal</li>
 *   <li>set of values</li>
 *   <li>range of values</li>
 *   <li>select clause</li>
 * </ul>
 */
public class SqlGivenItem extends SqlCall {
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("GIVEN", SqlKind.HINT) {
        @Override public SqlCall createCall(
            SqlLiteral functionQualifier,
            SqlParserPos pos,
            SqlNode... operands) {
          return new SqlGivenItem(pos,
              (SqlIdentifier) requireNonNull(operands[0], "name"),
              requireNonNull(operands[1], "value"),
              ((SqlLiteral) requireNonNull(operands[2], "givenType"))
                  .getValueAs(GivenType.class));
        }
      };


  //~ Enums ------------------------------------------------------------------

  /**
   * The given type.
   */
  public enum GivenType implements Symbolizable {
    /** Enumeration that  implements Symbolizable {
     /**
     * The given value is a simple identifier.
     */
    IDENTIFIER,
    /**
     * The given value is a numeric literal.
     */
    NUMERIC,
    /**
     * The given value is a string literal.
     */
    STRING,
    /**
     * The give value is a set of numeric or string literals.
     */
    ARRAY,
    /**
     * The given value is a range of numeric values specified by (min, max[, step]).
     */
    RANGE
  }

  //~ Instance fields --------------------------------------------------------

  private final SqlIdentifier name;
  private final SqlNode value;
  private final GivenType givenType;

  //~ Constructors -----------------------------------------------------------

  public SqlGivenItem(
      SqlParserPos pos,
      SqlIdentifier name,
      SqlNode value,
      GivenType givenType) {
    super(pos);
    this.name = Objects.requireNonNull(name, "name");
    this.value = Objects.requireNonNull(value, "value");
    this.givenType = Objects.requireNonNull(givenType, "givenType");
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(name, value, givenType.symbol(SqlParserPos.ZERO));
  }

  /** Returns the sql given name. */
  public SqlIdentifier getName() {
    return name;
  }

  /** Returns the given value. */
  public SqlNode getValue() {
    return value;
  }

  /** Returns the given type. */
  public GivenType getGivenType() {
    return givenType;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    name.unparse(writer, leftPrec, rightPrec);
    writer.keyword("=");
    value.unparse(writer, leftPrec, rightPrec);
  }
}

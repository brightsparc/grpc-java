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
import org.apache.calcite.sql.type.*;
import org.checkerframework.checker.nullness.qual.*;

import java.util.*;
import java.util.stream.*;

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
            @Nullable SqlLiteral functionQualifier,
            SqlParserPos pos,
            @Nullable SqlNode... operands) {
          return new SqlGivenItem(pos,
              (SqlIdentifier) operands[0],
              (SqlIdentifier) requireNonNull(operands[1], "name"),
              requireNonNull(operands[2], "value"),
              ((SqlLiteral) requireNonNull(operands[3], "givenType"))
                  .getValueAs(GivenType.class));
        }
      };


  //~ Enums ------------------------------------------------------------------

  /**
   * The given type.
   */
  public enum GivenType implements Symbolizable {
    /**
     * The given value is a simple identifier.
     */
    IDENTIFIER,
    /**
     * The given value is a binary value.
     */
    BINARY,
    /**
     * The given value is a numeric literal.
     */
    NUMERIC,
    /**
     * The given value is a string literal.
     */
    STRING,
    /**
     * The give value is an array of binary, numeric or string literals.
     */
    ARRAY,
    /**
     * The given value is an array of which we will sample from using a choice or grid search.
     */
    SAMPLE_ARRAY,
    /**
     * The given value is a range of integer values specified by (min, max[, step]).
     */
    RANGE_INT,
    /**
     * The given value is a range of real values specified by (min, max[, steps[, scale]]).
     */
    RANGE_REAL,
  }

  //~ Instance fields --------------------------------------------------------

  private final SqlIdentifier parent;
  private final SqlIdentifier name;
  private final SqlNode value;
  private final GivenType givenType;

  //~ Constructors -----------------------------------------------------------

  public static SqlIdentifier createIdentifier(SqlIdentifier parent, SqlIdentifier child,
      SqlParserPos pos) {
    if (parent == null) {
      return child;
    }
    return new SqlIdentifier(
        ImmutableList.<String>builder().addAll(parent.names).addAll(child.names).build(), pos);
  }

  public SqlGivenItem(
      SqlParserPos pos,
      SqlIdentifier parent,
      SqlIdentifier name,
      SqlNode value,
      GivenType givenType) {
    super(pos);
    this.parent = parent;
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

  /** Return the parent identifier. */
  public SqlIdentifier getParent() {
    return parent;
  }

  /** Returns the given name that is nested. */
  public SqlIdentifier getName() {
    return name;
  }

  /** Returns the given name as a type. */
  public <T extends Object> T getNestedNameAs(Class<T> clazz) {
    SqlIdentifier nested = createIdentifier(parent, name, name.getParserPosition());
    if (clazz.isInstance(nested)) {
      return clazz.cast(nested);
    }
    // If we are asking for a string, get the simple name, or use
    if (clazz == String.class) {
      if (nested.isSimple()) {
        return clazz.cast(nested.getSimple());
      }
      return clazz.cast(nested.toString());
    }
    throw new AssertionError("cannot cast " + nested + " as " + clazz);
  }

  /** Returns the given value. */
  public SqlNode getValue() {
    return value;
  }

  /** Returns the given value as a type. */
  public <T extends Object> T getValueAs(Class<T> clazz) {
    if (clazz.isInstance(value)) {
      return clazz.cast(value);
    }
    switch (givenType) {
    case BINARY:
      if (clazz == Boolean.class) {
        SqlLiteral lit = (SqlLiteral) value;
        if (lit.getTypeName() == SqlTypeName.BOOLEAN) {
          return lit.getValueAs(clazz);
        }
      }
      break;
    case IDENTIFIER:
      if (clazz == String.class) {
        SqlIdentifier id = (SqlIdentifier) value;
        if (id.isSimple()) {
          return clazz.cast(id.getSimple());
        }
        return clazz.cast(id.toString());
      }
      break;
    case STRING:
      return ((SqlLiteral) value).getValueAs(clazz);
    case NUMERIC:
      return ((SqlNumericLiteral) value).getValueAs(clazz);
    }
    throw new AssertionError("cannot cast " + value + " as " + clazz);
  }

  /** Returns the given array value as a list of type. */
  public <T extends Object> List<T> getArrayValueAs(Class<T> clazz) {
    if (givenType == GivenType.ARRAY) {
      return ((SqlCall) value).getOperandList().stream().map(value ->
          ((SqlLiteral) value).getValueAs(clazz)).collect(Collectors.toList());
    }
    throw new AssertionError("cannot cast array " + value + " as " + clazz);
  }

  /** Returns the given type. */
  public GivenType getGivenType() {
    return givenType;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    // NOTE: Don't write the parent here (as this will be named argument in unparse)
    name.unparse(writer, leftPrec, rightPrec);
    writer.keyword("=");
    value.unparse(writer, leftPrec, rightPrec);
  }
}

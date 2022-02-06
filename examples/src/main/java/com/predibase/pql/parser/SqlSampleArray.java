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
import java.util.stream.*;

/**
 * A <code>SqlSampleArray</code> is a node of a parse tree which represents
 * a sql given statement for the <code>SqlPredict</code> clause.
 *
 * <p>Basic given grammar is: <code>sample_choice(ARRAY[])</code> or
 * <code>sample_grid(ARRAY[])</code>.
 */
public class SqlSampleArray extends SqlCall {
  //~ Enums ------------------------------------------------------------------

  /**
   * The sample type.
   */
  public enum SampleType implements Symbolizable {
     /**
     * The default sample.
     */
     SAMPLE_AUTO,
    /**
     * One choice.
     */
    SAMPLE_CHOICE,
    /**
     * Grid search.
     */
    SAMPLE_GRID
  }

  //~ Instance fields --------------------------------------------------------

  private final SampleType sampleType;
  private final SqlNode array;

  //~ Constructors -----------------------------------------------------------

  public SqlSampleArray(
      SqlParserPos pos,
      SampleType scaleType,
      SqlNode array) {
    super(pos);
    this.sampleType = Objects.requireNonNull(scaleType, "scaleType");
    this.array = array;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlOperator getOperator() {
    return new SqlSpecialOperator(sampleType.name(), SqlKind.OTHER);
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(sampleType.symbol(SqlParserPos.ZERO), array);
  }

  /** Returns the array. */
  public SqlBasicCall getArray() {
    return (SqlBasicCall) array;
  }

  /** Returns the array of values. */
  public <T extends Object> List<T> getArrayValueAs(Class<T> clazz) {
    return ((SqlCall) array).getOperandList().stream().map(value ->
        ((SqlLiteral) value).getValueAs(clazz)).collect(Collectors.toList());
  }

  /** Returns the given type. */
  public SampleType getSampleType() {
    return sampleType;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.print(sampleType.name());
    // array will output brackets so don't need to add here
    array.unparse(writer, leftPrec, rightPrec);
  }
}

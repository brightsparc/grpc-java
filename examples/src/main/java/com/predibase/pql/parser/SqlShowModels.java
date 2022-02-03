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

/**
 * A <code>SqlShowModels</code> is a node of a parse tree which represents
 * the instruction to show models.
 */
public class SqlShowModels extends SqlShow {
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("SHOW MODELS", SqlKind.OTHER);

  //~ Instance fields --------------------------------------------------------

  public final SqlNode like;

  //~ Constructors -----------------------------------------------------------

  /** Creates a SqlShowModels. */
  public SqlShowModels(SqlParserPos pos, SqlNode like) {
    super(OPERATOR, pos);
    this.like = like;
  }

  //~ Methods ----------------------------------------------------------------

  @Pure
  public final SqlNode getLike() {
    return like;
  }

  /** Returns the like as a type. */
  public <T extends Object> T getLiekAs(Class<T> clazz) {
    if (clazz.isInstance(like)) {
      return clazz.cast(like);
    }
    if (clazz == String.class) {
      if (like instanceof SqlLiteral) {
        return ((SqlLiteral) like).getValueAs(clazz);
      }
    }
    throw new AssertionError("cannot cast " + like + " as " + clazz);
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(like);
  }

  @Override public void unparseCall(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("SHOW");
    writer.keyword("MODELS");
    if (like != null) {
      writer.keyword("LIKE");
      like.unparse(writer, leftPrec, rightPrec);
    }
  }

}

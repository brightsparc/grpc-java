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

/**
 * Parse tree for privilege statement.
 */
public abstract class SqlShow extends SqlDdl {
  //~ Constructors -----------------------------------------------------------

  SqlShow(SqlOperator operator, SqlParserPos pos) {
    super(operator, pos);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (!writer.inQuery()) {
      // If this SELECT is the topmost item in a sub-query, introduce a new
      // frame. (The topmost item in the sub-query might be a UNION or
      // ORDER. In this case, we don't need a wrapper frame.)
      final SqlWriter.Frame frame =
          writer.startList(SqlWriter.FrameTypeEnum.SUB_QUERY, "(", ")");
      this.unparseCall(writer, 0, 0);
      writer.endList(frame);
    } else {
      this.unparseCall(writer, leftPrec, rightPrec);
    }
  }

  protected abstract void unparseCall(SqlWriter writer, int leftPrec, int rightPrec);

}

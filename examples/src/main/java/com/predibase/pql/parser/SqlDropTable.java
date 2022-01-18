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

import java.util.*;

/**
 * Parse tree for {@code DROP TABLE} statement.
 */
public class SqlDropTable extends SqlDrop {
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("DROP TABLE", SqlKind.DROP_TABLE);

  public final SqlIdentifier name;

  /** Creates a SqlDropTable. */
  SqlDropTable(SqlParserPos pos, boolean ifExists, SqlIdentifier name) {
    super(OPERATOR, pos, ifExists);
    this.name = name;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("DROP");
    writer.keyword("TABLE");
    name.unparse(writer, leftPrec, rightPrec);
    if (ifExists) {
      writer.keyword("IF EXISTS");
    }
  }

}

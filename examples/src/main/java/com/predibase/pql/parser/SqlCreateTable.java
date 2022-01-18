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
import java.util.function.*;

/**
 * Parse tree for {@code CREATE TABLE} statement.
 */
public class SqlCreateTable extends SqlCreate {
  public final SqlIdentifier name;
  public final SqlNodeList columnList;
  public final SqlNode query;

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

  /** Creates a SqlCreateTable. */
  public SqlCreateTable(SqlParserPos pos, boolean replace, boolean ifNotExists,
      SqlIdentifier name, SqlNodeList columnList, SqlNode query) {
    super(OPERATOR, pos, replace, ifNotExists);
    this.name = Objects.requireNonNull(name, "name");
    this.columnList = columnList; // may be null
    this.query = query; // for "CREATE TABLE ... AS query"; may be null
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, columnList, query);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    if (getReplace()) {
      writer.keyword("OR REPLACE");
    }
    writer.keyword("TABLE");
    name.unparse(writer, leftPrec, rightPrec);
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    if (columnList != null) {
      SqlWriter.Frame frame = writer.startList("(", ")");
      forEachNameType((name, typeSpec) -> {
        writer.sep(",");
        name.unparse(writer, leftPrec, rightPrec);
        typeSpec.unparse(writer, leftPrec, rightPrec);
        if (Boolean.FALSE.equals(typeSpec.getNullable())) {
          writer.keyword("NOT NULL");
        }
      });
      writer.endList(frame);
    }
    if (query != null) {
      writer.keyword("AS");
      writer.newlineAndIndent();
      query.unparse(writer, 0, 0);
    }
  }

  /** Calls an action for each (name, type) pair from {@code columnList}, in which
   * they alternate. */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void forEachNameType(BiConsumer<SqlIdentifier, SqlDataTypeSpec> consumer) {
    final List list = columnList;
    Pair.forEach((List<SqlIdentifier>) Util.quotientList(list, 2, 0),
        Util.quotientList((List<SqlDataTypeSpec>) list, 2, 1), consumer);
  }
}

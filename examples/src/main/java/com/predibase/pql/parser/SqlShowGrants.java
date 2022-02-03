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
 * Parse tree for {@code SHOW GRANT} statement.
 */
public class SqlShowGrants extends SqlShow {
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("SHOW GRANTS", SqlKind.OTHER_DDL);

  //~ Instance fields --------------------------------------------------------

  public final SqlIdentifier name;
  public final SqlPrivilege.ObjectType objectType;
  public final SqlIdentifier to;
  public final SqlIdentifier of;

  //~ Constructors -----------------------------------------------------------

  /** Creates a SqlShowGrants. */
  SqlShowGrants(SqlPrivilege.ObjectType objectType, SqlIdentifier name,
      SqlIdentifier to, SqlIdentifier of,  SqlParserPos pos) {
    super(OPERATOR, pos);
    // Is either  "ON" <object type> <name>, "TO" <user or role>, or "OF" <user or role>
    this.objectType = objectType;
    this.name = name;
    this.to = to;
    this.of = of;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, objectType.symbol(SqlParserPos.ZERO), to, of);
  }

  @Override public void unparseCall(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("SHOW");
    writer.keyword("GRANTS");
    if (name != null) {
      writer.keyword("ON");
      writer.keyword(objectType.name());
      name.unparse(writer, leftPrec, rightPrec);
    } else if (to != null) {
      writer.keyword("TO");
      if (to instanceof SqlRoleIdentifier) {
        writer.keyword("ROLE");
      } else if (to instanceof SqlUserIdentifier) {
        writer.keyword("USER");
      }
      to.unparse(writer, leftPrec, rightPrec);
    } else if (of != null) {
      writer.keyword("OF");
      if (of instanceof SqlRoleIdentifier) {
        writer.keyword("ROLE");
      } else if (of instanceof SqlUserIdentifier) {
        writer.keyword("USER");
      }
      of.unparse(writer, leftPrec, rightPrec);
    }
  }
}

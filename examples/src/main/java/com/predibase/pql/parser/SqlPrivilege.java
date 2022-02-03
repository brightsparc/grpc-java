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
 * Parse tree for privilege statement.
 */
public abstract class SqlPrivilege extends SqlDdl {
  /**
   * The feature type.
   */
  public enum ObjectType implements Symbolizable {
    /**
     * Undefined object type.
     */
    UNDEFINED,
    /**
     * Engine object type.
     */
    ENGINE,
    /**
     * Connection object type.
     */
    CONNECTION,
    /**
     * Dataset objet type.
     */
    DATASET,
    /**
     * Model object type.
     */
    MODEL,
  }

  //~ Instance fields --------------------------------------------------------

  public final SqlNodeList privilegeList;
  public final ObjectType objectType;
  public final SqlNodeList onList;
  public final SqlNodeList roleList;
  public final SqlIdentifier resource;

  //~ Constructors -----------------------------------------------------------

  SqlPrivilege(SqlOperator operator, SqlNodeList privilegeList, ObjectType objectType,
      SqlNodeList onList, SqlNodeList roleList, SqlIdentifier resource, SqlParserPos pos) {
    super(operator, pos);
    this.privilegeList = privilegeList;
    this.objectType = objectType;
    this.onList = onList;
    this.roleList = roleList;
    this.resource = resource;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(privilegeList, objectType.symbol(SqlParserPos.ZERO),
        onList, roleList, resource);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(this.getOperator().getName());
    if (privilegeList != null) {
      privilegeList.unparse(writer, leftPrec, rightPrec);
      writer.keyword("ON");
      // TODO: Change this to add secured object list
      writer.keyword(objectType.name());
      onList.unparse(writer, leftPrec, rightPrec);
    } else if (roleList != null) {
      writer.keyword("ROLE");
      roleList.unparse(writer, leftPrec, rightPrec);
    }
    // Output TO of FROM depending on operator
    if (this.getOperator().getName().equals("GRANT")) {
      writer.keyword("TO");
    } else {
      writer.keyword("FROM");
    }
    if (resource instanceof SqlRoleIdentifier) {
      writer.keyword("ROLE");
    } else if (resource instanceof SqlUserIdentifier) {
      writer.keyword("USER");
    }
    resource.unparse(writer, leftPrec, rightPrec);
  }

}

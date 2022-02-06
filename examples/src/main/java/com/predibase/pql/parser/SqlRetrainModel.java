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
 * Parse tree for {@code RETRAIN MODEL} statement.
 */
public class SqlRetrainModel extends SqlCreateModel {
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("RETRAIN MODEL", SqlKind.OTHER_DDL);

  /** Creates a SqlRetrainModel with config only. */
  public SqlRetrainModel(SqlParserPos pos, SqlIdentifier name, SqlNode config) {
    super(pos, false, false, name, config, null, null);
  }

  /** Creates a SqlRetrainModel. */
  SqlRetrainModel(SqlParserPos pos,
      SqlIdentifier name, SqlNodeList featureList, SqlNodeList targetList,
      SqlNodeList preprocessing, SqlNodeList combiner, SqlNodeList trainer, SqlNodeList hyperopt) {
    super(pos, false, false, name, featureList, targetList,
        preprocessing, combiner, trainer, hyperopt, null, null);
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("RETRAIN");
    writer.keyword("MODEL");
    name.unparse(writer, leftPrec, rightPrec);
    unparseConfig(writer, leftPrec, rightPrec);
  }

}

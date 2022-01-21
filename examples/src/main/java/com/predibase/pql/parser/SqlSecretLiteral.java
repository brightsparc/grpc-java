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

/**
 * A SQL literal representing a secret value eg username or password.
 */
public class SqlSecretLiteral extends SqlCharStringLiteral {
  //~ Instance fields --------------------------------------------------------\

  final String secret;

  //~ Constructors -----------------------------------------------------------

  /**
   * Constructs a secret literal.
   */
  public SqlSecretLiteral(String charSet, String secret, SqlParserPos pos) throws ParseException {
    super(new NlsString(secret, charSet, null), pos);
    this.secret = secret;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    // TODO: return a sha1 hash of the value?
    writer.keyword("'****'");
  }

}

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

import java.util.regex.*;

/**
 * A SQL literal representing a java.util.regex.Pattern value.
 *
 * <p>Example: <code>CRON '0 9-17 * *'</code>
 *
 * The cron expression is made of the following elements, where * matches any:
 * <ul>
 *   <li>Minute (0 - 59)</li>
 *   <li>Hour (0 - 23)</li>
 *   <li>Day of month (1 - 31)</li>
 *   <li>Month (1 - 12)</li>
 *   <li>Day of week (0 - 6) (Sunday to Saturday)</li>
 * </ul>
 */
public class SqlCronLiteral extends SqlCharStringLiteral {
  /**
   * Regular expression to match cron expression.
   */
  private static final Pattern REGEX = Pattern.compile("(@(annually|yearly|monthly|weekly|daily"
      + "|hourly|reboot))|(@every (\\d+(ns|us|µs|ms|s|m|h))+)|((((\\d+,)+\\d+|(\\d+(\\/|-)\\d+)"
      + "|\\d+|\\*) ?){5,7})");

  //~ Instance fields --------------------------------------------------------\

  final String exp;

  //~ Constructors -----------------------------------------------------------

  /**
   * Constructs a cron literal.
   */
  public SqlCronLiteral(String charSet, String exp, SqlParserPos pos)
      throws IllegalArgumentException {
    super(new NlsString(exp, charSet, null), pos);
    if (REGEX.matcher(exp).matches()) {
      this.exp = exp;
    } else {
      throw new IllegalArgumentException(exp);
    }
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CRON");
    super.unparse(writer, leftPrec, rightPrec);
  }

}

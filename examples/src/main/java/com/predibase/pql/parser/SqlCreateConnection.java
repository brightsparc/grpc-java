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
import org.apache.calcite.sql.type.*;
import org.apache.calcite.util.*;
import org.checkerframework.dataflow.qual.*;

import java.util.*;

/**
 * A <code>SqlCreateConnection</code> is a node of a parse tree {@code CREATE CONNECTION}
 * which creates a connection with a given type, uri and credentials.
 *
 * <code>CREATE CONNECTION name
 *   CONNECTION_TYPE=S3
 *   CONNECTION_URI='s3://bucket/path'
 *   CREDENTIALS='arn:aws:iam::001234567890:role/myrole'
 *   ENABLED=true
 * </code>
 */
public class SqlCreateConnection extends SqlCreate {
  //~ Enums ------------------------------------------------------------------

  /**
   * The connection type.
   */
  public enum ConnectionType implements Symbolizable {
    /**
     * Azure Data Lake Storage.
     */
    ADLS,
    /**
     * Google Cloud Storage.
     */
    GCS,
    /**
     * Amazon S3.
     */
    S3,
    /**
     * MySQL database.
     */
    MYSQL,
    /**
     * PostgreSQL database.
     */
    POSTGRESQL,
    /**
     * Snowflake data warehouse.
     */
    SNOWFLAKE,
    /**
     * Amazon Redshift data warehouse.
     */
    REDSHIFT,
    /**
     * Google BigQuery data warehouse.
     */
    BIGQUERY,
  }

  //~ Instance fields --------------------------------------------------------

  public final SqlIdentifier name;
  public final ConnectionType connectionType;
  public SqlSecretLiteral accessKey;
  public SqlSecretLiteral secretKey;
  public SqlNode roleArn;
  public SqlSecretLiteral username;
  public SqlSecretLiteral password;
  public final SqlNode connectionUri;
  public final SqlLiteral enabled;

  //~ Static Fields -----------------------------------------------------------

  public static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE CONNECTION", SqlKind.OTHER_DDL);

  /** Creates a SqlCreateSchedule. */
  public SqlCreateConnection(SqlParserPos pos, boolean replace, boolean ifNotExists,
                             SqlIdentifier name, ConnectionType connectionType,
                             SqlSecretLiteral accessKey, SqlSecretLiteral secretKey, SqlNode roleArn,
                             SqlSecretLiteral username, SqlSecretLiteral password,
                             SqlNode connectionUri, SqlLiteral enabled) {
    super(OPERATOR, pos, replace, ifNotExists);
    this.name = Objects.requireNonNull(name, "name");
    this.connectionType = Objects.requireNonNull(connectionType, "connectionType");
    if (this.connectionType == ConnectionType.S3) {
      // For S3 connection, require access key and secret key, with optional role arn
      this.accessKey = Objects.requireNonNull(accessKey, "accessKey");
      this.secretKey = Objects.requireNonNull(secretKey, "secretKey");
      this.roleArn = roleArn;
      this.connectionUri = connectionUri; // optional
    } else if (this.connectionType == ConnectionType.ADLS) {
      this.connectionUri = connectionUri; // optional
    } else if (this.connectionType == ConnectionType.GCS) {
      this.connectionUri = connectionUri; // optional
    } else {
      // Add db's username and password credentials
      this.username = Objects.requireNonNull(username, "username");
      this.password = Objects.requireNonNull(password, "password");
      this.connectionUri = Objects.requireNonNull(connectionUri, "connectionUri");
    }
    this.enabled = enabled;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, connectionType.symbol(SqlParserPos.ZERO),
        accessKey, secretKey, roleArn, username, password, connectionUri, enabled);
  }

  @Pure
  public final SqlIdentifier getName() {
    return name;
  }

  /** Returns the given name as a type. */
  public <T extends Object> T getNameAs(Class<T> clazz) {
    if (clazz.isInstance(name)) {
      return clazz.cast(name);
    }
    // If we are asking for a string, get the simple name, or use
    if (clazz == String.class) {
      if (name.isSimple()) {
        return clazz.cast(name.getSimple());
      }
      return clazz.cast(name.toString());
    }
    throw new AssertionError("cannot cast " + name + " as " + clazz);
  }

  @Pure
  public final ConnectionType getType() {
    return connectionType;
  }

  @Pure
  public final SqlNode getUri() {
    return connectionUri;
  }

  @Pure
  public final SqlSecretLiteral getAccessKey() {
    return accessKey;
  }

  @Pure
  public final SqlSecretLiteral getSecretKey() {
    return secretKey;
  }

  @Pure
  public final SqlNode getRoleArn() {
    return roleArn;
  }

  @Pure
  public final SqlSecretLiteral getUsername() {
    return username;
  }

  @Pure
  public final SqlSecretLiteral getPassword() {
    return password;
  }

  @Pure
  public final SqlLiteral getEnabled() {
    return enabled;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    if (getReplace()) {
      writer.keyword("OR REPLACE");
    }
    writer.keyword("CONNECTION");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);
    // TODO: Consider if we want to create LPARAM/RPARAM
    SqlWriter.Frame frame = writer.startList("", "");
    writer.newlineAndIndent();
    writer.keyword("CONNECTION_TYPE");
    writer.keyword("=");
    writer.keyword(connectionType.toString());
    if (connectionType == ConnectionType.S3) {
      // Require access key and secret key, with optional role
      writer.newlineAndIndent();
      writer.keyword("AWS_ACCESS_KEY_ID");
      writer.keyword("=");
      accessKey.unparse(writer, leftPrec, rightPrec);
      writer.newlineAndIndent();
      writer.keyword("AWS_SECRET_ACCESS_KEY");
      writer.keyword("=");
      secretKey.unparse(writer, leftPrec, rightPrec);
      if (roleArn != null) {
        writer.newlineAndIndent();
        writer.keyword("AWS_ROLE_ARN");
        writer.keyword("=");
        roleArn.unparse(writer, leftPrec, rightPrec);
      }
    } else if (connectionType == ConnectionType.ADLS) {
      // TODO: Credentials
    } else if (connectionType == ConnectionType.GCS) {
      // TODO: Credentials
    } else {
      // DB username/password
      writer.newlineAndIndent();
      writer.keyword("USERNAME");
      writer.keyword("=");
      username.unparse(writer, leftPrec, rightPrec);
      writer.newlineAndIndent();
      writer.keyword("PASSWORD");
      writer.keyword("=");
      password.unparse(writer, leftPrec, rightPrec);
    }
    // Connection URI is optional
    if (connectionUri != null) {
      writer.newlineAndIndent();
      writer.keyword("CONNECTION_URI");
      writer.keyword("=");
      connectionUri.unparse(writer, leftPrec, rightPrec);
    }
    if (enabled.getTypeName() == SqlTypeName.BOOLEAN) {
      writer.newlineAndIndent();
      writer.keyword("ENABLED");
      writer.keyword("=");
      enabled.unparse(writer, leftPrec, rightPrec);
    }
    writer.endList(frame);
  }
}

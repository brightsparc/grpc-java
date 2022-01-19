/*
 * Copyright 2016 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.predibase.pql.api;

import io.grpc.*;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.*;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link PqlServer}.
 * For demonstrating how to write gRPC unit test only.
 * Not intended to provide a high code coverage or to test every major usecase.
 *
 * directExecutor() makes it easier to have deterministic tests.
 * However, if your implementation uses another thread and uses streaming it is better to use
 * the default executor, to avoid hitting bug #3084.
 *
 * <p>For more unit test examples see {@link io.grpc.examples.routeguide.RouteGuideClientTest} and
 * {@link io.grpc.examples.routeguide.RouteGuideServerTest}.
 */
@RunWith(JUnit4.class)
public class PqlServerTest {
  /**
   * This rule manages automatic graceful shutdown for the registered servers and channels at the
   * end of test.
   */
  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  /**
   * This rule allows specifying expected exceptions.
   */
  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  final String sqlStatement = "SELECT * FROM s1";

  /**
   * Verify native sql for target dialect
   */
  @Test
  public void testNativeMySql() throws IOException {
    ParseResponse response = getStub().parse(ParseRequest.newBuilder().setStatement(sqlStatement)
            .setTargetDialect(ParseRequest.TargetDialect.MYSQL).build());

    // Validate predict clause using target dialect
    assertEquals(ParseError.getDefaultInstance(), response.getParseError());
    assertEquals(ParseResponse.ClauseType.NATIVE_SQL, response.getClauseType());
    assertEquals("SELECT *\nFROM \"S1\"", response.getParsedSql());
    assertEquals(1, response.getTargetSqlCount());
    assertEquals("SELECT *\nFROM `S1`", response.getTargetSql(0));
  }

  final String predictStmt = String.format("PREDICT t1, t2 WITH explanation INTO d USING m GIVEN (%s)", sqlStatement);

  /**
   * Verify predict returns expected values for snowflake dialect.
   */
  @Test
  public void testPredictSnowflake() throws IOException {
    ParseResponse response = getStub().parse(ParseRequest.newBuilder().setStatement(predictStmt)
            .setTargetDialect(ParseRequest.TargetDialect.SNOWFLAKE).build());

    // Validate predict clause using target dialect
    assertEquals(ParseError.getDefaultInstance(), response.getParseError());
    assertEquals(ParseResponse.ClauseType.PREDICT, response.getClauseType());
    assertEquals("PREDICT (\"T1\", \"T2\") WITH EXPLANATION INTO \"D\"\n" +
            "USING \"M\"\n" +
            "GIVEN (SELECT *\n" +
            "FROM \"S1\")", response.getParsedSql());
    PredictClause predict = response.getClause().getPredictClause();
    assertEquals(2, predict.getTargetListCount());
    assertEquals("T1", predict.getTargetList(0));
    assertEquals("T2", predict.getTargetList(1));
    assertEquals(PredictClause.WithQualifier.EXPLANATION, predict.getWithQualifier());
    assertEquals("D", predict.getInto());
    assertEquals("M", predict.getModel());
    assertEquals(1, response.getTargetSqlCount());
    assertEquals("SELECT *\nFROM \"S1\"", response.getTargetSql(0));
  }

  /**
   * Verify predict returns expected values for mysql dialect.
   */
  @Test
  public void testPredictMySql() throws IOException {
    ParseResponse response = getStub().parse(ParseRequest.newBuilder().setStatement(predictStmt)
            .setTargetDialect(ParseRequest.TargetDialect.MYSQL).build());

    // Validate predict clause using target dialect
    assertEquals(ParseError.getDefaultInstance(), response.getParseError());
    assertEquals(ParseResponse.ClauseType.PREDICT, response.getClauseType());
    // Verify the parsed sql is still the same ANSI sql format
    assertEquals("PREDICT (\"T1\", \"T2\") WITH EXPLANATION INTO \"D\"\n" +
            "USING \"M\"\n" +
            "GIVEN (SELECT *\n" +
            "FROM \"S1\")", response.getParsedSql());
    assertEquals(1, response.getTargetSqlCount());
    assertEquals("SELECT *\nFROM `S1`", response.getTargetSql(0));
  }

  /**
   * Verify predict given returns a list of objects with the correct types.
   */
  @Test
  public void testPredictGivenItems() throws IOException {
    String predictGiven = predictStmt + ", i=id, s='s', f=0.1, a=ARRAY[1,2], r=given_range(1,100,10)";
    ParseResponse response = getStub().parse(ParseRequest.newBuilder().setStatement(predictGiven)
            .setTargetDialect(ParseRequest.TargetDialect.SNOWFLAKE).build());

    // Validate predict clause using target dialect
    assertEquals(ParseError.getDefaultInstance(), response.getParseError());
    assertEquals(ParseResponse.ClauseType.PREDICT, response.getClauseType());
    assertEquals("PREDICT (\"T1\", \"T2\") WITH EXPLANATION INTO \"D\"\n" +
            "USING \"M\"\n" +
            "GIVEN (SELECT *\n" +
            "FROM \"S1\"), " +
            "\"I\" = \"ID\", \"S\" = 's', \"F\" = 0.1, \"A\" = ARRAY[1, 2], \"R\" = GIVEN_RANGE (1, 100, 10)",
            response.getParsedSql());
    PredictClause predict = response.getClause().getPredictClause();
    // Verify we also have a select
    assertEquals(1, response.getTargetSqlCount());
    assertEquals("SELECT *\nFROM \"S1\"", response.getTargetSql(0));
    // Check the given list has all the items
    assertEquals(5, predict.getGivenListCount());
    assertEquals(GivenItem.GivenType.IDENTIFIER, predict.getGivenList(0).getType());
    assertEquals("ID", predict.getGivenList(0).getIdentifierValue(0));
    assertEquals(GivenItem.GivenType.STRING, predict.getGivenList(1).getType());
    assertEquals("s", predict.getGivenList(1).getStringValue(0));
    assertEquals(GivenItem.GivenType.NUMERIC, predict.getGivenList(2).getType());
    assertEquals(0.1, predict.getGivenList(2).getNumericValue(0), 0);
    assertEquals(GivenItem.GivenType.ARRAY, predict.getGivenList(3).getType());
    assertEquals(1, predict.getGivenList(3).getNumericValue(0), 0);
    assertEquals(2, predict.getGivenList(3).getNumericValue(1), 0);
    assertEquals(GivenItem.GivenType.RANGE, predict.getGivenList(4).getType());
    assertEquals(1, predict.getGivenList(4).getMinValue(), 0);
    assertEquals(100, predict.getGivenList(4).getMaxValue(), 0);
    assertEquals(10, predict.getGivenList(4).getStepValue(), 0);
  }

  /**
   * Verify predict returns correct parse message and properties
   */
  @Test
  public void testPredictParseError() throws IOException {
    // Define malformed predict query that is missing USING statement
    String statement = "PREDICT x GIVEN y";
    ParseResponse response = getStub().parse(ParseRequest.newBuilder()
            .setStatement(statement).setTargetDialect(ParseRequest.TargetDialect.SNOWFLAKE).build());

    // Validate predict clause using target dialect
    ParseError err = response.getParseError();
    assertNotEquals(ParseError.getDefaultInstance(), err);
    // Verify error message
    assertEquals("Encountered \"GIVEN\" at line 1, column 11.\n" +
            "Was expecting one of:\n" +
            "    \"INTO\" ...\n" +
            "    \"USING\" ...\n" +
            "    \"WITH\" ...\n" +
            "    \")\" ...\n" +
            "    \",\" ...\n" +
            "    \".\" ...\n" +
            "    ", err.getMessage());
    // Verify tokens
    assertEquals(6, err.getExpectedTokensCount());
    assertEquals("\")\"", err.getExpectedTokens(0));
    assertEquals("\",\"", err.getExpectedTokens(1));
    assertEquals("\".\"", err.getExpectedTokens(2));
    assertEquals("\"INTO\"", err.getExpectedTokens(3));
    assertEquals("\"USING\"", err.getExpectedTokens(4));
    assertEquals("\"WITH\"", err.getExpectedTokens(5));
    // Verify position
    assertEquals(1, err.getPosition().getLineNumber());
    assertEquals(1, err.getPosition().getEndLineNumber());
    assertEquals(11, err.getPosition().getColumnNumber());
    assertEquals(11, err.getPosition().getEndColumnNumber());
    assertEquals(ParseResponse.ClauseType.UNDEFINED, response.getClauseType());
  }

  /**
   * Verify that we return an exception for unsupported dialect
   */
  @Test
  public void testPredictUnsupportedDialect() throws IOException {
    exceptionRule.expect(StatusRuntimeException.class);

    // TODO: Validate the inner exception UnsupportedOperationException
    getStub().parse(ParseRequest.newBuilder()
            .setStatement(predictStmt)
            .setTargetDialect(ParseRequest.TargetDialect.UNDEFINED).build());
  }

  /**
   * Verify create s3 connection parses
   */
  @Test
  public void testS3Connection() throws IOException {
    String statement = "CREATE CONNECTION s3_connection "
            + "connection_type = S3 "
            + "aws_access_key_id = 'access_key'\n"
            + "aws_secret_access_key = 'secret_key'\n"
            + "aws_role_arn = 'arn:aws:iam::001234567890:role/myrole'\n"
            + "connection_uri = 's3://bucket/path'";
    ParseResponse response = getStub().parse(ParseRequest.newBuilder().setStatement(statement)
            .setTargetDialect(ParseRequest.TargetDialect.SNOWFLAKE).build());

    // Validate predict clause using target dialect
    assertEquals(ParseError.getDefaultInstance(), response.getParseError());
    assertEquals(ParseResponse.ClauseType.CREATE_CONNECTION, response.getClauseType());
    CreateConnectionClause conn = response.getClause().getCreateConnection();
    assertEquals(CreateConnectionClause.ConnectionType.S3, conn.getConnectionType());
    assertEquals("access_key", conn.getAccessKey());
    assertEquals("secret_key", conn.getSecretKey());
    assertEquals("arn:aws:iam::001234567890:role/myrole", conn.getRoleArn());
    assertEquals("s3://bucket/path", conn.getConnectionUri());
  }

  /**
   * Verify create db connection parses
   */
  @Test
  public void testDBConnection() throws IOException {
    String statement = "CREATE CONNECTION db_connection "
            + "connection_type = SNOWFLAKE "
            + "username = 'username'\n"
            + "password = 'password'\n"
            + "connection_uri = 'jdbc:snowflake://<account_identifier>.snowflakecomputing.com'";
    ParseResponse response = getStub().parse(ParseRequest.newBuilder().setStatement(statement)
            .setTargetDialect(ParseRequest.TargetDialect.SNOWFLAKE).build());

    // Validate predict clause using target dialect
    assertEquals(ParseError.getDefaultInstance(), response.getParseError());
    assertEquals(ParseResponse.ClauseType.CREATE_CONNECTION, response.getClauseType());
    CreateConnectionClause conn = response.getClause().getCreateConnection();
    assertEquals(CreateConnectionClause.ConnectionType.SNOWFLAKE, conn.getConnectionType());
    assertEquals("username", conn.getUsername());
    assertEquals("password", conn.getPassword());
    assertEquals("jdbc:snowflake://<account_identifier>.snowflakecomputing.com", conn.getConnectionUri());
  }

  /**
   * To test the server, make calls with a real stub using the in-process channel.
   * @return {@link ParserGrpc.ParserBlockingStub} for testing
   * @throws IOException
   */
  private ParserGrpc.ParserBlockingStub getStub() throws IOException {
    // Generate a unique in-process server name.
    String serverName = InProcessServerBuilder.generateName();

    // Create a server, add service, start, and register for automatic graceful shutdown.
    grpcCleanup.register(InProcessServerBuilder
        .forName(serverName).directExecutor().addService(new PqlParser()).build().start());

    // Create a client channel and register for automatic graceful shutdown.
    return ParserGrpc.newBlockingStub(
        grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));
  }
}

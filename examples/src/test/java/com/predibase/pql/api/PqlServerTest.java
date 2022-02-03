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
    assertEquals("SELECT *\nFROM `S1`", response.getClause().getNativeSqlClause().getQuery());
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
    assertEquals("SELECT *\nFROM \"S1\"", predict.getQuery(0));
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
    PredictClause predict = response.getClause().getPredictClause();
    assertEquals("SELECT *\nFROM `S1`", predict.getQuery(0));
  }

  /**
   * Verify predict given returns a list of objects with the correct types.
   */
  @Test
  public void testPredictGivenItems() throws IOException {
    String predictGiven = predictStmt + ", i=id, s='s', f=0.1, a=ARRAY[1,2], r=range(1,100,10)";
    ParseResponse response = getStub().parse(ParseRequest.newBuilder().setStatement(predictGiven)
            .setTargetDialect(ParseRequest.TargetDialect.SNOWFLAKE).build());

    // Validate predict clause using target dialect
    assertEquals(ParseError.getDefaultInstance(), response.getParseError());
    assertEquals(ParseResponse.ClauseType.PREDICT, response.getClauseType());
    assertEquals("PREDICT (\"T1\", \"T2\") WITH EXPLANATION INTO \"D\"\n" +
            "USING \"M\"\n" +
            "GIVEN (SELECT *\n" +
            "FROM \"S1\"), " +
            "\"I\" = \"ID\", \"S\" = 's', \"F\" = 0.1, \"A\" = ARRAY[1, 2], \"R\" = RANGE (1, 100, 10)",
            response.getParsedSql());
    PredictClause predict = response.getClause().getPredictClause();
    // Verify we also have a select
    assertEquals("SELECT *\nFROM \"S1\"", predict.getQuery(0));
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
    assertEquals(10, predict.getGivenList(4).getStepsValue(), 0);
  }

  /**
   * Verify predict returns correct parse message and properties
   */
  @Test
  public void testPredictParseError() throws IOException {
    // Define malformed predict query that is missing USING statement
    String statement = "PREDICT x USING y";
    ParseResponse response = getStub().parse(ParseRequest.newBuilder()
            .setStatement(statement).setTargetDialect(ParseRequest.TargetDialect.SNOWFLAKE).build());

    // Validate predict clause using target dialect
    ParseError err = response.getParseError();
    assertNotEquals(ParseError.getDefaultInstance(), err);
    // Verify error message
    assertEquals("Encountered \"<EOF>\" at line 1, column 17.\n" +
            "Was expecting one of:\n" +
            "    \"VERSION\" ...\n" +
            "    \"GIVEN\" ...\n" +
            "    \".\" ...\n" +
            "    ", err.getMessage());
    // Verify tokens
    assertEquals(3, err.getExpectedTokensCount());
    assertEquals("\".\"", err.getExpectedTokens(0));
    assertEquals("\"GIVEN\"", err.getExpectedTokens(1));
    assertEquals("\"VERSION\"", err.getExpectedTokens(2));
    // Verify position
    assertEquals(1, err.getPosition().getLineNumber());
    assertEquals(1, err.getPosition().getEndLineNumber());
    assertEquals(17, err.getPosition().getColumnNumber());
    assertEquals(17, err.getPosition().getEndColumnNumber());
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
    assertEquals("S3_CONNECTION", conn.getName());
    assertEquals(CreateConnectionClause.ConnectionType.S3, conn.getConnectionType());
    // Validate secret properties
    assertEquals(4, conn.getSecretPropertiesCount());
    assertEquals("access_key", conn.getSecretPropertiesOrDefault(
            PqlParser.SecretProperties.AWS_ACCESS_KEY_ID.name(), null));
    assertEquals("secret_key", conn.getSecretPropertiesOrDefault(
            PqlParser.SecretProperties.AWS_SECRET_ACCESS_KEY.name(), null));
    assertEquals("arn:aws:iam::001234567890:role/myrole", conn.getSecretPropertiesOrDefault(
            PqlParser.SecretProperties.AWS_ROLE_ARN.name(), null));
    assertEquals("s3://bucket/path", conn.getSecretPropertiesOrDefault(
            PqlParser.SecretProperties.CONNECTION_URI.name(), null));
  }

  /**
   * Verify create db connection.
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
    assertEquals("DB_CONNECTION", conn.getName());
    assertEquals(CreateConnectionClause.ConnectionType.SNOWFLAKE, conn.getConnectionType());
    assertEquals("username", conn.getSecretPropertiesOrDefault(
            PqlParser.SecretProperties.USERNAME.name(), null));
    assertEquals("password", conn.getSecretPropertiesOrDefault(
            PqlParser.SecretProperties.PASSWORD.name(), null));
    assertEquals("jdbc:snowflake://<account_identifier>.snowflakecomputing.com",
            conn.getSecretPropertiesOrDefault(PqlParser.SecretProperties.CONNECTION_URI.name(), null));
  }

  /**
   * Verify create s3 dataset.
   */
  @Test
  public void testS3Dataset() throws IOException {
    String statement = "create dataset s3_dataset "
            + "into s3_target dataset_format ( type='parquet' )"
            + "from s3_source dataset_format ( type='csv' )";
    ParseResponse response = getStub().parse(ParseRequest.newBuilder().setStatement(statement)
            .setTargetDialect(ParseRequest.TargetDialect.SNOWFLAKE).build());

    // Validate predict clause using target dialect
    assertEquals(ParseError.getDefaultInstance(), response.getParseError());
    assertEquals(ParseResponse.ClauseType.CREATE_DATASET, response.getClauseType());
    CreateDatasetClause ds = response.getClause().getCreateDataset();
    assertEquals("S3_DATASET", ds.getName());
    // Get target properties
    assertEquals("S3_TARGET", ds.getTarget().getTableRef());
    assertEquals(1, ds.getTarget().getFormatPropertiesCount());
    assertEquals("parquet", ds.getTarget().getFormatPropertiesOrDefault("TYPE", null));
    // Get source properties
    assertEquals("S3_SOURCE", ds.getSource().getTableRef());
    assertEquals(1, ds.getSource().getFormatPropertiesCount());
    assertEquals("csv", ds.getSource().getFormatPropertiesOrDefault("TYPE", null));
  }

  /**
   * Verify create db dataset as select.
   */
  @Test
  public void testDBDatasetView() throws IOException {
    String statement = "create dataset db_dataset as "
            + "select * from db_connection_name.table_name "
            + "where date_col < now()";
    ParseResponse response = getStub().parse(ParseRequest.newBuilder().setStatement(statement)
            .setTargetDialect(ParseRequest.TargetDialect.SNOWFLAKE).build());

    // Validate predict clause using target dialect
    assertEquals(ParseError.getDefaultInstance(), response.getParseError());
    assertEquals(ParseResponse.ClauseType.CREATE_DATASET, response.getClauseType());
    CreateDatasetClause ds = response.getClause().getCreateDataset();
    assertEquals("DB_DATASET", ds.getName());
    // Verify target and source are empty
    assertEquals(DatasetRef.getDefaultInstance(), ds.getTarget());
    assertEquals(DatasetRef.getDefaultInstance(), ds.getSource());
    // Verify query
    assertEquals("SELECT *\n" +
            "FROM \"DB_CONNECTION_NAME\".\"TABLE_NAME\"\n" +
            "WHERE \"DATE_COL\" < \"NOW\"()", ds.getQuery());
  }

  /**
   * Test creating a model from config.
   */
  @Test
  public void testCreateModelFromConfig() throws IOException {
    String statement = "CREATE MODEL m CONFIG '{ \"x\": 1 }' FROM ds";
    ParseResponse response = getStub().parse(ParseRequest.newBuilder().setStatement(statement)
            .setTargetDialect(ParseRequest.TargetDialect.SNOWFLAKE).build());

    // Validate predict clause using target dialect
    assertEquals(ParseError.getDefaultInstance(), response.getParseError());
    assertEquals(ParseResponse.ClauseType.CREATE_MODEL, response.getClauseType());
    CreateModelClause model = response.getClause().getCreateModel();
    assertEquals("M", model.getName());
    // Get config value as String
    String config = model.getConfig().getValue().toStringUtf8();
    assertEquals("{ \"x\": 1 }", config);
  }

  /**
   * Test creating a model from config.
   */
  @Test
  public void testCreateModelFromProperties() throws IOException {
    String statement = "create model m (\n"
            + "n numeric encoder (x=y, y='1', a.b.c=.1), " // TODO: Change this to not support nested
            + "b binary decoder (z=1), "
            + "s set, " // DO we want to use this name given its meaning in SQL?
            + "t text"
            + ") "
            + "processor ( force_split=true, split_probabilities=ARRAY[0.7, 0.1, 0.2] )"
            + "combiner ( type='concat' ) "
            + "trainer ( epoch=range(1, 100, 10, LINEAR) ) "
            + "target b "
            + "from ds";
    System.out.println(statement);
    ParseResponse response = getStub().parse(ParseRequest.newBuilder().setStatement(statement)
            .setTargetDialect(ParseRequest.TargetDialect.SNOWFLAKE).build());

    // Validate predict clause using target dialect
    assertEquals(ParseError.getDefaultInstance(), response.getParseError());
    assertEquals(ParseResponse.ClauseType.CREATE_MODEL, response.getClauseType());
    CreateModelClause model = response.getClause().getCreateModel();
    assertEquals("M", model.getName());
    // Get config as ModelConfig class
    ModelConfig config = model.getConfig().unpack(ModelConfig.class);
    // Verify input features
    assertEquals(3, config.getInputFeatureCount());
    Feature i1 = config.getInputFeature(0);
    assertEquals(Feature.FeatureType.NUMERIC, i1.getType());
    assertEquals("N", i1.getName());
    assertEquals(3, i1.getEncoderCount());
    GivenItem i1e1 = i1.getEncoderOrDefault("X", null);
    assertNotNull(i1e1);
    assertEquals(GivenItem.GivenType.IDENTIFIER, i1e1.getType());
    assertEquals("Y", i1e1.getIdentifierValue(0));
    // Verify output features
    assertEquals(1, config.getOutputFeatureCount());
    Feature o1 = config.getOutputFeature(0);
    assertEquals(Feature.FeatureType.BINARY, o1.getType());
    assertEquals("B", o1.getName());
    assertEquals(1, o1.getDecoderCount());
    GivenItem o1d1 = o1.getDecoderOrDefault("Z", null);
    assertNotNull(o1d1);
    assertEquals(GivenItem.GivenType.NUMERIC, o1d1.getType());
    assertEquals(1, o1d1.getNumericValue(0), 0);
    // Verify processing
    assertEquals(2, config.getProcessorCount());
    GivenItem p1 = config.getProcessorOrDefault("FORCE_SPLIT", null);
    assertNotNull(p1);
    assertEquals(GivenItem.GivenType.BINARY, p1.getType());
    assertEquals(true, p1.getBoolValue(0));
    GivenItem p2 = config.getProcessorOrDefault("SPLIT_PROBABILITIES", null);
    assertNotNull(p2);
    assertEquals(GivenItem.GivenType.ARRAY, p2.getType());
    assertEquals(0.7, p2.getNumericValue(0), 0);
    assertEquals(0.1, p2.getNumericValue(1), 0);
    assertEquals(0.2, p2.getNumericValue(2), 0);
    // Verify combiner
    assertEquals(1, config.getCombinerCount());
    // Verify trainer
    assertEquals(1, config.getTrainerCount());
    GivenItem t1 = config.getTrainerOrDefault("EPOCH", null);
    assertNotNull(t1);
    assertEquals(GivenItem.GivenType.RANGE, t1.getType());
    assertEquals(1, t1.getMinValue(), 0);
    assertEquals(100, t1.getMaxValue(), 0);
    assertEquals(10, t1.getStepsValue(), 0);
    // Verify source
    assertEquals("DS", model.getSource().getTableRef());
  }

  /**
   * Test creating a model from config.
   */
  @Test
  public void testCreateModelAsSelect() throws IOException {
    String statement = "CREATE MODEL m (s text, b binary) TARGET b AS SELECT * FROM ds";
    ParseResponse response = getStub().parse(ParseRequest.newBuilder().setStatement(statement)
            .setTargetDialect(ParseRequest.TargetDialect.SNOWFLAKE).build());

    // Validate predict clause using target dialect
    assertEquals(ParseError.getDefaultInstance(), response.getParseError());
    assertEquals(ParseResponse.ClauseType.CREATE_MODEL, response.getClauseType());
    CreateModelClause model = response.getClause().getCreateModel();
    // Verify create as select
    assertEquals("SELECT *\nFROM \"DS\"", model.getQuery());
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

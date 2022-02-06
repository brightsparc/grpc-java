package com.predibase.pql.api;

import com.google.protobuf.*;
import com.predibase.pql.parser.*;
import io.grpc.stub.*;
import io.prometheus.client.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.sql.type.*;

import java.util.*;
import java.util.logging.*;
import java.util.stream.*;

public class PqlParser extends ParserGrpc.ParserImplBase {
    // TODO: Use structured logging
    private static final Logger logger = Logger.getLogger(PqlServer.class.getName());

    // Create prom metrics for requests, payload size, and latency
    static final Counter requests = Counter.build()
            .name("requests").help("Total requests.")
            .labelNames("operator").register();
    static final Counter parseErrors = Counter.build()
            .name("parse_errors").help("Total parse errors.").register();
    static final Summary receivedBytes = Summary.build()
            .name("requests_size_bytes").help("Request size in bytes.").register();
    static final Histogram requestLatency = Histogram.build()
            .name("requests_latency_seconds").help("Request latency in seconds.").register();

    /** Secret properties keys */
    public enum SecretProperties implements Symbolizable {
        /** Secret properties key for S3 AWS_ACCESS_KEY_ID. */
        AWS_ACCESS_KEY_ID,
        /** Secret properties key for S3 AWS_SECRET_ACCESS_KEY. */
        AWS_SECRET_ACCESS_KEY,
        /** Secret properties key for S3 AWS_ROLE_ARN. */
        AWS_ROLE_ARN,
        /** Secret properties key for ADLS AZURE_SAS_TOKEN. */
        AZURE_SAS_TOKEN,
        /** Secret properties key for database USERNAME. */
        USERNAME,
        /** Secret properties key for database PASSWORD. */
        PASSWORD,
        /** Secret properties key for database PASSWORD. */
        CONNECTION_URI,
    }

    // Create static methods for parser factory and source dialect
    final static SqlParser.Config pqlParser = SqlParser.config()
            .withParserFactory(ExtensionSqlParserImpl.FACTORY);

    // Define the source dialect as using double quotes for identifiers and single quotes for literals
    final static SqlDialect sourceDialect =  new SqlDialect(SqlDialect.EMPTY_CONTEXT
            .withDatabaseProduct(SqlDialect.DatabaseProduct.UNKNOWN)
            .withLiteralQuoteString("'")
            .withIdentifierQuoteString("\""));

    protected SqlParser getParser(String sql) {
        return SqlParser.create(sql, sourceDialect.configureParser(pqlParser));
    }

    protected SqlDialect getDialect(ParseRequest.TargetDialect targetDialect) throws UnsupportedOperationException {
        switch (targetDialect) {
            case SNOWFLAKE: return SqlDialect.DatabaseProduct.SNOWFLAKE.getDialect();
            case MYSQL: return SqlDialect.DatabaseProduct.MYSQL.getDialect();
            case POSTGRESQL: return SqlDialect.DatabaseProduct.POSTGRESQL.getDialect();
            default:
                throw new UnsupportedOperationException(
                        String.format("Target dialect %s not supported", targetDialect));
        }
    }

    @Override
    public void parse(ParseRequest request, StreamObserver<ParseResponse> responseObserver) {
        logger.fine(String.format("Parse request: %s", request.getStatement()));
        Histogram.Timer requestTimer = requestLatency.startTimer();

        ParseResponse response;
        try {
            // Parse query for statement
            SqlNode node = getParser(request.getStatement()).parseQuery();
            // Get target dialect (TODO: consider de-coupling from all statements)
            SqlDialect targetDialect = getDialect(request.getTargetDialect());

            // Return the response from parsed sql
            if (node instanceof SqlCall) {
                response = parseSql((SqlCall) node, targetDialect);
            } else {
                throw new UnsupportedOperationException(String.format("Node %s not supported", node.getKind()));
            }
        } catch (SqlParseException e) {
            // Print stacktrace with error
            e.printStackTrace();
            // Create the error with position
            ParseError error = ParseError.newBuilder()
                    .setMessage(e.getMessage())
                    .addAllExpectedTokens(e.getExpectedTokenNames())
                    .setPosition(ParserPos.newBuilder()
                            .setLineNumber(e.getPos().getLineNum())
                            .setColumnNumber(e.getPos().getColumnNum())
                            .setEndLineNumber(e.getPos().getEndLineNum())
                            .setEndColumnNumber(e.getPos().getColumnNum())).build();
            // Return the response with error and undefined clause type
            response = ParseResponse.newBuilder()
                    .setParseError(error)
                    .setClauseType(ParseResponse.ClauseType.UNDEFINED).build();
            parseErrors.inc();
        } finally {
            receivedBytes.observe(request.getSerializedSize());
            requestTimer.observeDuration();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private ParseResponse parseSql(SqlCall node, SqlDialect targetDialect) {
        String opName = node.getOperator().getName().replace(" ", "_");
        logger.info(String.format("Parse operator: %s ", opName));
        requests.labels(opName).inc();

        if (node.getKind() == SqlKind.OTHER || node.getKind() == SqlKind.OTHER_DDL) {
            // Parse the clause type based on operator name
            switch (ParseResponse.ClauseType.valueOf(opName)) {
                case CREATE_CONNECTION: return parseCreateConnection((SqlCreateConnection) node);
                case CREATE_DATASET: return parseCreateDataset((SqlCreateDataset) node, targetDialect);
                case CREATE_MODEL: return parseCreateModel((SqlCreateModel) node, targetDialect);
                case PREDICT: return parsePredict((SqlPredict) node, targetDialect);
                default:
                    throw new UnsupportedOperationException(
                            String.format("Operator %s not implemented", opName));
            }
        } else {
            return parseNativeSql(node, targetDialect);
        }
    }

    private ParseResponse parseNativeSql(SqlNode node, SqlDialect targetDialect) {
        // Return native sql in the target dialect for the source node
        String targetSql = node.toSqlString(targetDialect).toString();
        return ParseResponse.newBuilder()
                .setClauseType(ParseResponse.ClauseType.NATIVE_SQL)
                .setClause(Clause.newBuilder().setNativeSqlClause(
                        NativeSqlClause.newBuilder().setQuery(targetSql)))
                .setParsedSql(node.toSqlString(sourceDialect).toString()).build();
    }

    private ParseResponse parseCreateConnection(SqlCreateConnection node) {
        CreateConnectionClause.Builder builder = CreateConnectionClause.newBuilder()
                .setName(node.getNameAs(String.class))
                .setConnectionType(CreateConnectionClause.ConnectionType.valueOf(node.getConnectionType().toString()));

        // If blob store, set access and secret key, else set username and password
        switch (node.getConnectionType()) {
            case S3:
                builder.putSecretProperties(SecretProperties.AWS_ACCESS_KEY_ID.name(),
                        node.getAccessKey().getValueAs(String.class));
                builder.putSecretProperties(SecretProperties.AWS_SECRET_ACCESS_KEY.name(),
                        node.getSecretKey().getValueAs(String.class));
                if (node.getRoleArn() != null) {
                    builder.putSecretProperties(SecretProperties.AWS_ROLE_ARN.name(),
                            node.getRoleArn().getValueAs(String.class));
                }
                break;
            case ADLS:
                builder.putSecretProperties(SecretProperties.AZURE_SAS_TOKEN.name(),
                        node.getUsername().getValueAs(String.class));
                break;
            case GCS:
                break;
            default:
                builder.putSecretProperties(SecretProperties.USERNAME.name(),
                        node.getUsername().getValueAs(String.class));
                builder.putSecretProperties(SecretProperties.PASSWORD.name(),
                        node.getPassword().getValueAs(String.class));
                break;
        }

        if (node.getConnectionUri() != null) {
            builder.putSecretProperties(SecretProperties.CONNECTION_URI.name(),
                    ((SqlLiteral) node.getConnectionUri()).getValueAs(String.class));
        }

        return ParseResponse.newBuilder()
                .setClauseType(ParseResponse.ClauseType.CREATE_CONNECTION)
                .setClause(Clause.newBuilder().setCreateConnection(builder.build()))
                .setParsedSql(node.toSqlString(sourceDialect).getSql())
                .build();
    }

    private ParseResponse parseCreateDataset(SqlCreateDataset node, SqlDialect targetDialect) {
        CreateDatasetClause.Builder builder = CreateDatasetClause.newBuilder()
                .setName(node.getNameAs(String.class));
        if (node.getTargetRef() != null) {
            builder.setTarget(parseDatasetRef(node.getTargetRef()));
        }
        if (node.getSourceRef() != null) {
            builder.setSource(parseDatasetRef(node.getSourceRef()));
        }
        if (node.getQuery() != null) {
            builder.setQuery(node.getQuery().toSqlString(targetDialect).toString());
        }
        return ParseResponse.newBuilder()
                .setClauseType(ParseResponse.ClauseType.CREATE_DATASET)
                .setClause(Clause.newBuilder().setCreateDataset(builder.build()))
                .setParsedSql(node.toSqlString(sourceDialect).getSql())
                .build();
    }

    private DatasetRef parseDatasetRef(SqlDatasetRef node) {
        DatasetRef.Builder builder = DatasetRef.newBuilder()
                .setTableRef(parseIdentifier(node.getTableRef()));
        if (node.getFormat() != null) {
            node.forEachFormat((k, v) ->
                    builder.putFormatProperties(parseIdentifier(k), v.getValueAs(String.class)));
        }
        return builder.build();
    }

    private ParseResponse parseCreateModel(SqlCreateModel node, SqlDialect targetDialect) {
        CreateModelClause.Builder builder = CreateModelClause.newBuilder()
                .setName(node.getNameAs(String.class));
        if (node.getConfig() != null) {
            String config = ((SqlLiteral) node.getConfig()).getValueAs(String.class);
            builder.setConfig(Any.newBuilder().setValue(ByteString.copyFromUtf8(config)));
        } else {
            // Create a new ModelConfig builder for setting specific properties that map closely to JSON string
            ModelConfig.Builder config = ModelConfig.newBuilder();
            // Get the targets
            List<String> targets = node.getTargetList().stream().map(t ->
                    parseIdentifier(t)).collect(Collectors.toList());
            node.getFeatureList().forEach(f -> {
                // TODO: Check if feature is or ARRAY or RANGE, and if so, add to hyperopt instead of input/output
                Feature feature = parseFeature(f);
                if (targets.contains(feature.getName())) {
                    config.addOutputFeatures(feature);
                } else {
                    config.addInputFeatures(feature);
                }
            });
            // Add preprocessing
            node.getPreprocessing().forEach(item ->
                    config.putPreprocessing(item.getNestedNameAs(String.class), parseGivenItem(item)));
            // Add combiner
            node.getCombiner().forEach(item ->
                    config.putCombiner(item.getNestedNameAs(String.class), parseGivenItem(item)));
            // Add combiner
            node.getTrainer().forEach(item ->
                    config.putTrainer(item.getNestedNameAs(String.class), parseGivenItem(item)));
            // Add hyperopt
            node.getHyperopt().forEach(item ->
                    config.putHyperopt(item.getNestedNameAs(String.class), parseGivenItem(item)));
            // Pack the config
            builder.setConfig(Any.pack(config.build()));
        }
        if (node.getSourceRef() != null) {
            builder.setSource(parseDatasetRef(node.getSourceRef()));
        }
        if (node.getQuery() != null) {
            builder.setQuery(node.getQuery().toSqlString(targetDialect).toString());
        }
        return ParseResponse.newBuilder()
                .setClauseType(ParseResponse.ClauseType.CREATE_MODEL)
                .setClause(Clause.newBuilder().setCreateModel(builder.build()))
                .setParsedSql(node.toSqlString(sourceDialect).getSql())
                .build();
    }

    private Feature parseFeature(SqlFeature feature) {
        Feature.Builder builder = Feature.newBuilder()
                .setName(feature.getNameAs(String.class))
                .setType(Feature.FeatureType.valueOf(feature.getGivenType().toString()));
        // NOTE: Try using a ANY parser for feature process and see what happens with serializer
        feature.getPreprocessing().forEach(item ->
                builder.putPreprocessing(item.getNestedNameAs(String.class), parseGivenItem(item)));
        feature.getEncoder().forEach(item ->
                builder.putEncoder(item.getNestedNameAs(String.class), parseGivenItem(item)));
        feature.getDecoder().forEach(item ->
                builder.putDecoder(item.getNestedNameAs(String.class), parseGivenItem(item)));
        return builder.build();
    }

    private ParseResponse parsePredict(SqlPredict predict, SqlDialect targetDialect) {
        // Add required fields for predict
        PredictClause.Builder builder = PredictClause.newBuilder()
                .setPredictType(PredictClause.PredictType.valueOf(predict.getPredictType().toString()))
                .addAllTargetList(predict.getTargetList().stream().map(t ->
                        parseIdentifier(t)).collect(Collectors.toList()))
                .setModel(parseIdentifier(predict.getModel().getName()));

        // Add optional fields
        if (predict.getWithQualifier() != null) {
            builder.setWithQualifier(PredictClause.WithQualifier.valueOf(predict.getWithQualifier().toString()));
        }
        if (predict.getModel().getVersion() > 0) {
            builder.setVersion(predict.getModel().getVersion());
        }
        if (predict.getInto() != null) {
            builder.setInto(predict.getInto().toString());
        }

        // Add any given items required
        builder.addAllGivenList(predict.getGivenItems().map(item ->
                parseGivenItem(item)).collect(Collectors.toList()));

        // Add any queries
        builder.addAllQuery(predict.getGivenSelect()
                .map(s -> s.toSqlString(targetDialect).toString())
                .collect(Collectors.toList()));

        return ParseResponse.newBuilder()
                .setClauseType(ParseResponse.ClauseType.PREDICT)
                .setClause(Clause.newBuilder().setPredictClause(builder.build()).build())
                .setParsedSql(predict.toSqlString(sourceDialect).getSql())
                .build();
    }

    private GivenItem parseGivenItem(SqlGivenItem item) throws UnsupportedOperationException {
        // Get the node name and type
        GivenItem.Builder builder = GivenItem.newBuilder()
                //.setName(item.getNestedNameAs(String.class))
                .setType(GivenItem.GivenType.valueOf(item.getGivenType().toString()));

        // Get the value as target class
        switch (item.getGivenType()) {
            case IDENTIFIER:
                return builder.addIdentifierValue(item.getValueAs(String.class)).build();
            case BINARY:
                return builder.addBoolValue(item.getValueAs(Boolean.class)).build();
            case NUMERIC:
                return builder.addNumericValue(item.getValueAs(Double.class)).build();
            case STRING:
                return builder.addStringValue(item.getValueAs(String.class)).build();
            case ARRAY:
            case SAMPLE_ARRAY:
                SqlBasicCall arr;
                if (item.getValue() instanceof SqlSampleArray) {
                    // TODO: Figure out a way to use getArrayValueAs
                    SqlSampleArray sample = ((SqlSampleArray) item.getValue());
                    builder.setSampleType(GivenItem.SampleType.valueOf(sample.getSampleType().toString()));
                    arr = sample.getArray();
                } else {
                    arr = (SqlBasicCall) item.getValue();
                }
                arr.getOperandList().forEach(a -> {
                    if (a instanceof SqlNumericLiteral) {
                        SqlNumericLiteral arrItem = (SqlNumericLiteral) a;
                        builder.addNumericValue(arrItem.getValueAs(Double.class));
                    } else if (a instanceof SqlLiteral) {
                        SqlLiteral arrItem = (SqlLiteral) a;
                        if (arrItem.getTypeName() == SqlTypeName.BOOLEAN) {
                            builder.addBoolValue(arrItem.getValueAs(Boolean.class));
                        } else {
                            builder.addStringValue(arrItem.getValueAs(String.class));
                        }
                    } else {
                        throw new UnsupportedOperationException(String.format(
                                "Unexpected array type: %s", a.getKind()));
                    }
                });
                return builder.build();
            case RANGE_INT:
                SqlRangeInt ri = (SqlRangeInt) item.getValue();
                builder.setMinValue(ri.getMin());
                builder.setMaxValue(ri.getMax());
                if (ri.getStep() > 0) {
                    builder.setStepValue(ri.getStep());
                }
                return builder.build();
            case RANGE_REAL:
                SqlRangeReal rr = (SqlRangeReal) item.getValue();
                builder.setMinValue(rr.getMin().getValueAs(Double.class));
                builder.setMaxValue(rr.getMax().getValueAs(Double.class));
                if (rr.getSteps() != null) {
                    builder.setStepValue(rr.getSteps().getValueAs(Double.class));
                }
                if (rr.getScaleType() != SqlRangeReal.ScaleType.AUTO) {
                    builder.setScaleType(GivenItem.ScaleType.valueOf(rr.getScaleType().toString()));
                }
                return builder.build();
            default:
                throw new UnsupportedOperationException(String.format(
                        "Unexpected identifier type: %s", item.getGivenType()));
        }
    }

    private String parseIdentifier(SqlIdentifier identifier) {
        if (identifier.isSimple()) {
            return identifier.getSimple();
        } else {
            return identifier.toString();
        }
    }

}
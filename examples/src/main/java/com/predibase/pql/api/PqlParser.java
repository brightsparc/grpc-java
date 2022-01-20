package com.predibase.pql.api;

import com.google.protobuf.*;
import com.predibase.pql.parser.*;
import io.grpc.stub.*;
import io.prometheus.client.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.util.*;

import java.util.*;
import java.util.function.*;
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
                .setConnectionType(CreateConnectionClause.ConnectionType.valueOf(node.getType().toString()));

        // If blob store, set access and secret key, else set username and password
        switch (node.getType()) {
            case S3:
                builder.setAccessKey(node.getAccessKey().getValueAs(String.class));
                builder.setSecretKey(node.getSecretKey().getValueAs(String.class));
                if (node.getRoleArn() != null) {
                    builder.setRoleArn(((SqlLiteral) node.getRoleArn()).getValueAs(String.class));
                }
                break;
            case ADLS:
            case GCS:
                // TODO: Add credentials
                break;
            default:
                builder.setUsername(node.getUsername().getValueAs(String.class));
                builder.setPassword(node.getPassword().getValueAs(String.class));
        }

        if (node.getUri() != null) {
            builder.setConnectionUri(((SqlLiteral) node.getUri()).getValueAs(String.class));
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
            String targetSql = node.getQuery().toSqlString(targetDialect).toString();
            builder.setQuery(targetSql);
        }
        return ParseResponse.newBuilder()
                .setClauseType(ParseResponse.ClauseType.CREATE_DATASET)
                .setClause(Clause.newBuilder().setCreateDataset(builder.build()))
                .setParsedSql(node.toSqlString(sourceDialect).getSql())
                .build();
    }

    private DatasetRef parseDatasetRef(SqlDatasetRef node) {
        DatasetRef.Builder builder = DatasetRef.newBuilder()
                .setTableRef(parseIdentifier(node.getTableRef()))
                .setDatasetUri(node.getUri().getValueAs(String.class));
        if (node.getFormat() != null) {
            // Enumerate over the list of Identifier/StringLiteral pairs to
            final List list = (SqlNodeList) node.getFormat();
            Pair.forEach((List<SqlIdentifier>) Util.quotientList(list, 2, 0),
                    Util.quotientList((List<SqlLiteral>) list, 2, 1), (k, v) ->
                            builder.putFormat(parseIdentifier(k), v.getValueAs(String.class))
            );
        }
        return builder.build();
    }

    private ParseResponse parseCreateModel(SqlCreateModel node, SqlDialect targetDialect) {
        CreateModelClause.Builder builder = CreateModelClause.newBuilder()
                .setName(node.getNameAs(String.class));
        if (node.getConfig() != null) {
            builder.setConfig(((SqlLiteral) node.getConfig()).getValueAs(String.class));
        } else {
            // Add features using the feature list builder
            builder.addAllFeatureList(node.getFeatureList().stream().map(f ->
                    parseFeature(f)).collect(Collectors.toList()));
            // Add combiner
            node.getCombiner().forEach(item ->
                    builder.putCombiner(item.getNameAs(String.class), parseGivenItem(item)));
            // Add combiner
            node.getTrainer().forEach(item ->
                    builder.putTrainer(item.getNameAs(String.class), parseGivenItem(item)));
            // Add split by as string list
            builder.addAllSplitByList(node.getSplitBy().stream().map(sb -> sb.toString()).collect(Collectors.toList()));
            //builder.setSourceRef(node.getSourceRef().getTableRef().getSimple());
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
        feature.getEncoder().forEach(item ->
                builder.putEncoder(item.getNameAs(String.class), parseGivenItem(item)));
        feature.getEncoder().forEach(item ->
                builder.putDecoder(item.getNameAs(String.class), parseGivenItem(item)));
        return builder.build();
    }

    private ParseResponse parsePredict(SqlPredict predict, SqlDialect targetDialect) {
        // Get target list
        List<String> targetList = predict.getTargetList().stream().map(t ->
            parseIdentifier((SqlIdentifier) t)
        ).collect(Collectors.toList());

        // Add required fields for predict
        PredictClause.Builder builder = PredictClause.newBuilder()
                .setPredictType(PredictClause.PredictType.valueOf(predict.getPredictType().toString()))
                .addAllTargetList(targetList)
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
        // For each given type parse the node value to and set on builder
        GivenItem.Builder builder = GivenItem.newBuilder()
                .setType(GivenItem.GivenType.valueOf(item.getGivenType().toString()));

        switch (item.getGivenType()) {
            case IDENTIFIER:
                SqlIdentifier identifier = (SqlIdentifier) item.getValue();
                return builder.addIdentifierValue(parseIdentifier(identifier)).build();
            case NUMERIC:
                SqlNumericLiteral numValue = (SqlNumericLiteral) item.getValue();
                return builder.addNumericValue(numValue.getValueAs(Double.class)).build();
            case STRING:
                SqlLiteral strValue = (SqlLiteral) item.getValue();
                return builder.addStringValue(strValue.getValueAs(String.class)).build();
            case ARRAY:
                SqlBasicCall arr = (SqlBasicCall) item.getValue();
                arr.getOperandList().forEach(a -> {
                    if (a instanceof SqlNumericLiteral) {
                        SqlNumericLiteral arrItem = (SqlNumericLiteral) a;
                        builder.addNumericValue(arrItem.getValueAs(Double.class));
                    } else if (a instanceof SqlLiteral) {
                        SqlLiteral arrItem = (SqlLiteral) item.getValue();
                        builder.addStringValue(arrItem.getValueAs(String.class));
                    } else {
                        throw new UnsupportedOperationException(String.format(
                                "Unexpected array type: %s", a.getKind()));
                    }
                });
                return builder.build();
            case RANGE:
                SqlGivenRange rng = (SqlGivenRange) item.getValue();
                builder.setMinValue(rng.getMin().getValueAs(Double.class));
                builder.setMaxValue(rng.getMax().getValueAs(Double.class));
                if (rng.getStep() != null) {
                    builder.setStepValue(rng.getStep().getValueAs(Double.class));
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
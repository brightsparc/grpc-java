package com.predibase.pql.api;

import com.predibase.pql.parser.*;
import io.grpc.stub.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.*;

import java.util.*;
import java.util.logging.*;
import java.util.stream.*;

public class PqlParser  extends ParserGrpc.ParserImplBase {
    // TODO: Use structured logging
    private static final Logger logger = Logger.getLogger(PqlServer.class.getName());

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
        logger.info(String.format("Got request %s", request.getStatement()));

        ParseResponse response;
        try {
            // Parse query for statement
            SqlNode node = getParser(request.getStatement()).parseQuery();
            // Get target dialect
            SqlDialect targetDialect = getDialect(request.getTargetDialect());

            // Return the response from parsed sql
            response = parseSql(node, targetDialect);
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
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private ParseResponse parseSql(SqlNode node, SqlDialect targetDialect) {
        if (node.getKind() == SqlKind.OTHER && node instanceof SqlCall) {
            // Get clause type from operator name
            String opName = ((SqlCall) node).getOperator().getName().replace(" ", "_");
            logger.info("Matched operator " + opName);

            // Parse the clause type based on operator name
            switch (ParseResponse.ClauseType.valueOf(opName)) {
                case PREDICT: return parsePredict((SqlPredict) node, targetDialect);
                default:
                    throw new UnsupportedOperationException(
                            String.format("Operator %s not supported", opName));
            }

        } else {
            // Return native sql in the target dialect for the source node
            return ParseResponse.newBuilder()
                    .setClauseType(ParseResponse.ClauseType.NATIVE_SQL)
                    .setParsedSql(node.toSqlString(sourceDialect).toString())
                    .addTargetSql(node.toSqlString(targetDialect).toString()).build();
        }
    }

    private ParseResponse parsePredict(SqlPredict predict, SqlDialect dialect) {
        // Get target list
        List<String> targetList = predict.getTargetList().stream().map(t -> {
            SqlIdentifier target = (SqlIdentifier) t;
            return target.getSimple();
        }).collect(Collectors.toList());

        // TODO: Push down code to return simple types to classes

        // Add required fields for predict
        PredictClause.Builder builder = PredictClause.newBuilder()
                .setPredictType(PredictClause.PredictType.valueOf(predict.getPredictType().toString()))
                .addAllTargetList(targetList)
                .setModel(predict.getModel().getName().getSimple());

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
        builder.addAllGivenList(parsePredictGiven(predict));

        // Return the target sql list
        List<String> targetSqlList = predict.getGivenSelect()
                .map(s -> s.toSqlString(dialect).toString())
                .collect(Collectors.toList());

        return ParseResponse.newBuilder()
                .setClauseType(ParseResponse.ClauseType.PREDICT)
                .setClause(Clause.newBuilder().setPredictClause(builder.build()).build())
                .setParsedSql(predict.toSqlString(sourceDialect).getSql())
                .addAllTargetSql(targetSqlList)
                .build();
    }

    private List<GivenItem> parsePredictGiven(SqlPredict predict) throws UnsupportedOperationException {
        List<SqlGivenItem> items = predict.getGivenItems().collect(Collectors.toList());

        return items.stream().map(item -> {
            GivenItem.Builder builder = GivenItem.newBuilder()
                    .setType(GivenItem.GivenType.valueOf(item.getGivenType().toString()));
            // For each given type parse the node value to add the property to the list
            switch (item.getGivenType()) {
                case IDENTIFIER:
                    SqlIdentifier identifier = (SqlIdentifier) item.getValue();
                    builder.addIdentifierValue(identifier.getSimple());
                    break;
                case NUMERIC:
                    SqlNumericLiteral numValue = (SqlNumericLiteral) item.getValue();
                    builder.addNumericValue(numValue.getValueAs(Double.class));
                    break;
                case STRING:
                    SqlLiteral strValue = (SqlLiteral) item.getValue();
                    builder.addStringValue(strValue.getValueAs(String.class));
                    break;
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
                    break;
                case RANGE:
                    SqlGivenRange rng = (SqlGivenRange) item.getValue();
                    builder.setMinValue(rng.getMin().getValueAs(Double.class));
                    builder.setMaxValue(rng.getMax().getValueAs(Double.class));
                    if (rng.getStep() != null) {
                        builder.setStepValue(rng.getStep().getValueAs(Double.class));
                    }
                    break;
                default:
                    throw new UnsupportedOperationException(String.format(
                            "Unexpected identifier type: %s", item.getGivenType()));
            }
            return builder.build();
        }).collect(Collectors.toList());
    }

}
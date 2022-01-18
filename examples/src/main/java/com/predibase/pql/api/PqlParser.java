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
                .setModel(predict.getModel().getName().getSimple())
                .setVersion(predict.getModel().getVersion()); // TODO: Default to 1

        // Add optional fields
        if (predict.getTable() != null) {
            builder.setTable(predict.getTable().toString());
        }
        if (predict.getWithQualifier() != null) {
            // TODO: Turn WithQualifier into enum
            //builder.setWithQualifier(predict.getWithQualifier())
        }

        // Return a list of target
        List<String> targetSqls = predict.getGivenSelect()
                .map(s -> s.toSqlString(dialect).toString())
                .collect(Collectors.toList());

        return ParseResponse.newBuilder()
                .setClauseType(ParseResponse.ClauseType.PREDICT)
                .setClause(Clause.newBuilder().setPredictClause(builder.build()).build())
                .setParsedSql(predict.toSqlString(sourceDialect).getSql())
                .addAllTargetSql(targetSqls)
                .build();
    }
}
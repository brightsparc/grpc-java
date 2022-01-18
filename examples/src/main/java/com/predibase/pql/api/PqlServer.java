/*
 * Copyright 2015 The gRPC Authors
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

import com.predibase.pql.parser.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.*;

// TODO: Attempt to import .sql
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.*;

/**
 * Server that manages startup/shutdown of a {@code Greeter} server.
 */
public class PqlServer {
    private static final Logger logger = Logger.getLogger(PqlServer.class.getName());

    private Server server;

    private void start() throws IOException {
        /* The port on which the server should run */
        int port = 50051;
        server = ServerBuilder.forPort(port)
                .addService(new ParserImpl())
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    PqlServer.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        final PqlServer server = new PqlServer();
        server.start();
        server.blockUntilShutdown();
    }

    static class ParserImpl extends ParserGrpc.ParserImplBase {

        protected SqlDialect getDialect() {
            // TODO: Get dialect from calcite
            final SqlDialect.Context context = SqlDialect.EMPTY_CONTEXT
                    .withDatabaseProduct(SqlDialect.DatabaseProduct.UNKNOWN)
                    .withLiteralQuoteString("'")
                    .withIdentifierQuoteString("\"");
            return new SqlDialect(context);
        }

        protected SqlParser ansiSqlParser(String sql, SqlDialect dialect) {
            // Create a new parser from sql string given a dialog and parser factory
            final SqlParser.Config configBuilder =
                    SqlParser.config().withParserFactory(ExtensionSqlParserImpl.FACTORY);
            return SqlParser.create(sql, dialect.configureParser(configBuilder));
        }

        public ParserImpl() {
        }

        @Override
        public void parse(ParseRequest request, StreamObserver<ParseResponse> responseObserver) {
            // TODO: Should we through exception, or return error response in GRPC?
            ParseResponse response;

            logger.info(String.format("Got request %s", request.getStatement()));

            try {
                // TODO: Get Dialect from request
                SqlDialect dialect = this.getDialect();
                SqlNode node = this.ansiSqlParser(request.getStatement(), dialect).parseQuery();

                // Default to null
                ParseResponse.ClauseType clauseType = ParseResponse.ClauseType.UNDEFINED;
                String targetSql = null;
                Clause clause = null;

                if (node.getKind() == SqlKind.OTHER) {

                    if (node instanceof SqlCall) {
                        // Get clause type from operator name
                        String opName = ((SqlCall) node).getOperator().getName().replace(" ", "_");
                        logger.info("Operator " + opName);
                        clauseType = ParseResponse.ClauseType.valueOf(opName);
                    }

                    // TODO: Switch on the instance type here (operator name)
                    if (node instanceof SqlPredict) {
                        SqlPredict predict = (SqlPredict) node;

                        List<String> targetList = predict.getTargetList().stream().map(t -> {
                            SqlIdentifier target = (SqlIdentifier) t;
                            return target.getSimple();
                        }).collect(Collectors.toList());

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

                        // Turn the given section into SQL
                        targetSql = predict.getGiven().toSqlString(dialect).toString();

                        // Set the clause
                        clause = Clause.newBuilder().setPredictClause(builder.build()).build();
                    }
                }

                response = ParseResponse.newBuilder()
                        .setClauseType(clauseType)
                        .setClauseProps(clause)
                        .setTargetSql(targetSql)
                        .build();

            } catch (SqlParseException e) {
                e.printStackTrace();
                // TODO: Set an error in the parse response
                response = ParseResponse.getDefaultInstance();
            }

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}

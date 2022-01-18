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

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A simple client that requests a greeting from the {@link PqlServer}.
 */
public class PqlClient {
    private static final Logger logger = Logger.getLogger(PqlClient.class.getName());

    private final ParserGrpc.ParserBlockingStub blockingStub;

    /** Construct client for accessing Pql server using the existing channel. */
    public PqlClient(Channel channel) {
        // 'channel' here is a Channel, not a ManagedChannel, so it is not this code's responsibility to
        // shut it down.

        // Passing Channels to code makes code easier to test and makes it easier to reuse Channels.
        blockingStub = ParserGrpc.newBlockingStub(channel);
    }

    /** Say parse to server. */
    public ParseResponse parse(String statement, ParseRequest.TargetDialect targetDialect) {
        logger.info(String.format("Will try to parse %s for dialect %s ...",
                statement, targetDialect));
        ParseRequest request = ParseRequest.newBuilder()
                .setStatement(statement).setTargetDialect(targetDialect).build();
        ParseResponse response;
        try {
            response = blockingStub.parse(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return null;
        }
        logger.info(String.format("Clause type %s, parsed sql: %s",
                response.getClauseType(), response.getParsedSql()));
        return response;
    }

    /**
     * Greet server. If provided, the first element of {@code args} is the name to use in the
     * greeting. The second argument is the target server.
     */
    public static void main(String[] args) throws Exception {
        String statement = "PREDICT Survived USING titanic_model GIVEN SELECT a, b FROM titanic";
        ParseRequest.TargetDialect dialect =  ParseRequest.TargetDialect.valueOf("SNOWFLAKE");
        // Access a service running on the local machine on port 50051
        String target = "localhost:50051";
        // Allow passing in the user and target strings as command line arguments
        if (args.length > 0) {
            if ("--help".equals(args[0])) {
                System.err.println("Usage: statement targetDialect");
                System.err.println("");
                System.err.println("  statement: The PQL statement");
                System.err.println("  targetDialect: The target dialect to parse for");
                System.exit(1);
            }
            statement = args[0];
            dialect = ParseRequest.TargetDialect.valueOf(args[1]);
        }

        // Create a communication channel to the server, known as a Channel. Channels are thread-safe
        // and reusable. It is common to create channels at the beginning of your application and reuse
        // them until the application shuts down.
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext()
                .build();
        try {
            PqlClient client = new PqlClient(channel);
            client.parse(statement, dialect);
        } finally {
            // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
            // resources the channel should be shut down when it will no longer be used. If it may be used
            // again leave it running.
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}

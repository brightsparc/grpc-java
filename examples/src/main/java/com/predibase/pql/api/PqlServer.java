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

import io.grpc.*;
import io.prometheus.client.*;
import me.dinowernli.grpc.prometheus.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;


/**
 * Server that manages startup/shutdown of a {@code Greeter} server.
 */
public class PqlServer {
    private static final Logger logger = Logger.getLogger(PqlServer.class.getName());

    private Server server;
    private PrometheusServer prometheusServer;

    private void start() throws IOException {
        // Create the metrics port
        // TODO: Add tracing interceptor
        int grpcPort = 50051;
        MonitoringServerInterceptor monitoringInterceptor =
                MonitoringServerInterceptor.create(Configuration.cheapMetricsOnly());
        server = ServerBuilder.forPort(grpcPort)
                .addService(ServerInterceptors.intercept(new PqlParser(), monitoringInterceptor))
                .build()
                .start();

        // Create the metrics server
        int metricsPort = 8081;
        prometheusServer = new PrometheusServer(CollectorRegistry.defaultRegistry, metricsPort);
        prometheusServer.start();

        logger.info(String.format("Server started, grpc listening on %d, metrics on %d", grpcPort, metricsPort));
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
            prometheusServer.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
            prometheusServer.shutdown();
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

}

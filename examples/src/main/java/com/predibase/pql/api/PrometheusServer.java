// from: https://github.com/saturnism/grpc-by-example-java/blob/master/zipkin-prometheus-example/src/main/java/com/example/grpc/server/PrometheusServer.java
package com.predibase.pql.api;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;

import java.io.OutputStreamWriter;

/**
 * Created by rayt on 10/8/16.
 */
public class PrometheusServer {
    private final CollectorRegistry registry;
    private final int port;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private Channel channel;

    public PrometheusServer(CollectorRegistry registry, int port) {
        this.registry = registry;
        this.port = port;
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
    }

    public void start() {
        final ServerBootstrap bootstrap = new ServerBootstrap();

        bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.DEBUG)).childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        ChannelPipeline pipeline = socketChannel.pipeline();
                        pipeline.addLast("decoder", new HttpRequestDecoder());
                        pipeline.addLast("encoder", new HttpResponseEncoder());
                        pipeline.addLast("prometheus", new SimpleChannelInboundHandler<Object>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object o)
                                    throws Exception {
                                if (!(o instanceof HttpRequest)) {
                                    return;
                                }

                                HttpRequest request = (HttpRequest) o;

                                if (!"/metrics".equals(request.uri())) {
                                    final FullHttpResponse response = new DefaultFullHttpResponse(
                                            HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND);
                                    channelHandlerContext.writeAndFlush(response)
                                            .addListener(ChannelFutureListener.CLOSE);
                                    return;
                                }

                                if (!HttpMethod.GET.equals(request.method())) {
                                    final FullHttpResponse response = new DefaultFullHttpResponse(
                                            HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_ACCEPTABLE);
                                    channelHandlerContext.writeAndFlush(response)
                                            .addListener(ChannelFutureListener.CLOSE);
                                    return;
                                }

                                ByteBuf buf = Unpooled.buffer();
                                ByteBufOutputStream os = new ByteBufOutputStream(buf);
                                OutputStreamWriter writer = new OutputStreamWriter(os);
                                TextFormat.write004(writer, registry.metricFamilySamples());
                                writer.close();
                                os.close();

                                final FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                                        HttpResponseStatus.OK, buf);
                                response.headers().set(HttpHeaderNames.CONTENT_TYPE, TextFormat.CONTENT_TYPE_004);
                                channelHandlerContext.writeAndFlush(response)
                                        .addListener(ChannelFutureListener.CLOSE);
                            }
                        });

                    }
                });

        try {
            this.channel = bootstrap.bind(this.port).sync().channel();
        } catch (InterruptedException e) {
            // do nothing
        }
    }

    public void awaitTermination() {
        try {
            this.channel.closeFuture().sync();
        } catch (InterruptedException e) {
            // do nothing
        }
    }

    public void shutdown() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }
}
package com.grpc.example;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import com.grpc.example.PingExampleGrpc.PingExampleImplBase;
import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

/** Application entry point. */
public final class Main {

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  private static final Metadata.Key<String> TRACE_SEQ_KEY =
      Metadata.Key.of("trace-seq", ASCII_STRING_MARSHALLER);

  private Main() {}

  /**
   * Runs the application.
   *
   * @param args command-line arguments
   */
  public static void main(final String... args) throws InterruptedException, IOException {
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();

    final Server server =
        ServerBuilder.forPort(50051)
            .addService(
                new PingExampleImplBase() {
                  @Override
                  public void ping(
                      PingRequest request, StreamObserver<PingResponse> responseObserver) {
                    responseObserver.onNext(PingResponse.newBuilder().build());
                    responseObserver.onCompleted();
                  }
                })
            .addService(ProtoReflectionService.newInstance())
            .intercept(logging())
            .build();

    System.err.println("*** starting gRPC Server");

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  System.err.println("*** shutting down gRPC Server since JVM is shutting down");
                  server.shutdown();
                  System.err.println("*** Server shut down");
                }));

    server.start();
    server.awaitTermination();
  }

  private static ServerInterceptor logging() {
    var seq = new AtomicInteger(0);
    return new ServerInterceptor() {
      @Override
      public <ReqT, RespT> Listener<ReqT> interceptCall(
          ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        var remoteSeq = headers.get(TRACE_SEQ_KEY);
        var seqNr = seq.incrementAndGet();

        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(
            next.startCall(
                new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
                  @Override
                  public void sendMessage(final RespT message) {
                    super.sendMessage(message);
                    logger.info("{} - sendMessage", seq);
                  }

                  @Override
                  public void close(final Status status, final Metadata trailers) {
                    super.close(status, trailers);
                    logger.info(
                        "{} trace-seq: {} - closed with status code: {}, desc: {}",
                        seqNr,
                        remoteSeq,
                        status.getCode(),
                        status.getDescription());
                  }

                  @Override
                  public void sendHeaders(final Metadata headers) {
                    super.sendHeaders(headers);
                    logger.info("{} - sendHeader", seqNr);
                  }
                },
                headers)) {

          @Override
          public void onHalfClose() {
            super.onHalfClose();
            logger.info("{} - onHalfClose", seqNr);
          }

          @Override
          public void onMessage(final ReqT message) {
            super.onMessage(message);
            logger.info("{} - onMessage", seqNr);
          }

          @Override
          public void onCancel() {
            super.onCancel();
            logger.info("{} - onCancel", seqNr);
          }

          @Override
          public void onComplete() {
            super.onComplete();
            logger.info("{} - onComplete", seqNr);
          }
        };
      }
    };
  }
}

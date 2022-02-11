package com.grpc.example.client;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import com.google.common.base.Stopwatch;
import com.grpc.example.PingExampleGrpc;
import com.grpc.example.PingRequest;
import com.grpc.example.PingResponse;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.okhttp.OkHttpChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.perfmark.PerfMark;
import io.perfmark.traceviewer.TraceEventViewer;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Main {

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  private static final Metadata.Key<String> TRACE_SEQ_KEY =
      Metadata.Key.of("trace-seq", ASCII_STRING_MARSHALLER);

  private Main() {}

  public static void main(final String... args) throws InterruptedException, IOException {
    ManagedChannel channel;
    if ("okhttp".equals(System.getProperty("com.grpc.example.transport", "netty"))) {
      logger.info("Using okhttp transport");
      channel = OkHttpChannelBuilder.forAddress("127.0.0.1", 50051).usePlaintext().build();
    } else {
      logger.info("Using netty transport");
      channel = NettyChannelBuilder.forAddress("127.0.0.1", 50051).usePlaintext().build();
    }

    while (true) {
      var state = channel.getState(true);
      if (ConnectivityState.READY == state) {
        break;
      }
    }

    PerfMark.setEnabled(true);

    var stub = PingExampleGrpc.newStub(channel);
    var slowCalls = new AtomicInteger(0);
    var countDownLatch = new CountDownLatch(1000);

    for (int i = 0; i < 1000; i++) {
      var started = Stopwatch.createStarted();
      var seqStr = String.valueOf(i);

      logger.info("starting call with trace-seq: {}", seqStr);

      var metadata = new Metadata();
      metadata.put(TRACE_SEQ_KEY, seqStr);
      stub.withDeadlineAfter(100, TimeUnit.MILLISECONDS)
          .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
          .ping(
              PingRequest.newBuilder().build(),
              new StreamObserver<PingResponse>() {
                @Override
                public void onNext(PingResponse pingResponse) {}

                @Override
                public void onError(Throwable throwable) {
                  countDownLatch.countDown();
                  started.stop();
                  var status = Status.fromThrowable(throwable);
                  logger.info("{} Status code: {}", seqStr, status.getCode());
                  logger.info("{} Status description: {}", seqStr, status.getDescription());
                  if (status.getCode() == Code.DEADLINE_EXCEEDED) {
                    slowCalls.incrementAndGet();
                  }
                }

                @Override
                public void onCompleted() {
                  countDownLatch.countDown();
                  started.stop();
                  var elapsed = started.elapsed(TimeUnit.MILLISECONDS);
                  logger.info("{} - call latency {} ms", seqStr, elapsed);
                }
              });
    }

    awaitTermination(channel, countDownLatch, slowCalls);
  }

  private static void awaitTermination(
      final ManagedChannel channel,
      final CountDownLatch countDownLatch,
      final AtomicInteger slowCalls)
      throws InterruptedException, IOException {
    countDownLatch.await(60, TimeUnit.SECONDS);
    channel.shutdown();
    channel.awaitTermination(60, TimeUnit.SECONDS);
    logger.info("{} calls didn't finish within the deadline", slowCalls.get());
    TraceEventViewer.writeTraceHtml();
  }
}

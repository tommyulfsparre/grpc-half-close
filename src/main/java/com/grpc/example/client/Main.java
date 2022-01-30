package com.grpc.example.client;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import com.google.common.base.Stopwatch;
import com.grpc.example.PingExampleGrpc;
import com.grpc.example.PingRequest;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.MetadataUtils;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Main {

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  private static final Metadata.Key<String> TRACE_SEQ_KEY =
      Metadata.Key.of("trace-seq", ASCII_STRING_MARSHALLER);

  private Main() {}

  public static void main(final String... args) throws InterruptedException {
    var executor = Executors.newFixedThreadPool(100);
    var channel = ManagedChannelBuilder.forAddress("127.0.0.1", 50051).usePlaintext().build();
    while (true) {
      var state = channel.getState(true);
      if (ConnectivityState.READY == state) {
        break;
      }
    }

    var stub = PingExampleGrpc.newBlockingStub(channel);
    var seq = new AtomicInteger(0);
    for (int i = 0; i < 100; i++) {
      executor.submit(
          () -> {
            var started = Stopwatch.createStarted();
            var seqStr = String.valueOf(seq.incrementAndGet());
            try {
              var metadata = new Metadata();
              metadata.put(TRACE_SEQ_KEY, seqStr);
              stub.withDeadlineAfter(100, TimeUnit.MILLISECONDS)
                  .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
                  .ping(PingRequest.newBuilder().build());
            } catch (Exception ex) {
              var status = Status.fromThrowable(ex);
              logger.info("{} Status code: {}", seqStr, status.getCode());
              logger.info("{} Status description: {}", seqStr, status.getDescription());
            } finally {
              started.stop();
              logger.info(
                  "{} - call latency {} ms", seqStr, started.elapsed(TimeUnit.MILLISECONDS));
            }
          });
    }

    awaitTermination(executor, channel);
  }

  private static void awaitTermination(final ExecutorService executor, final ManagedChannel channel)
      throws InterruptedException {
    executor.shutdown();
    executor.awaitTermination(60, TimeUnit.SECONDS);
    channel.shutdown();
    channel.awaitTermination(60, TimeUnit.SECONDS);
  }
}

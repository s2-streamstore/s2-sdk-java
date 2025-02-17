package org.example.app;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.channel.ManagedChannelFactory;
import s2.client.StreamClient;
import s2.config.AppendRetryPolicy;
import s2.config.Config;
import s2.config.Endpoints;
import s2.types.AppendInput;
import s2.types.AppendOutput;
import s2.types.AppendRecord;

public class ManagedAppendSessionDemo {

  private static final Logger logger =
      LoggerFactory.getLogger(ManagedAppendSessionDemo.class.getName());

  public static void main(String[] args) throws Exception {
    final var authToken = System.getenv("S2_AUTH_TOKEN");
    final var basinName = System.getenv("S2_BASIN");
    final var streamName = System.getenv("S2_STREAM");
    if (authToken == null) {
      throw new IllegalStateException("S2_AUTH_TOKEN not set");
    }
    if (basinName == null) {
      throw new IllegalStateException("S2_BASIN not set");
    }
    if (streamName == null) {
      throw new IllegalStateException("S2_STREAM not set");
    }

    var config =
        Config.newBuilder(authToken)
            .withEndpoints(Endpoints.fromEnvironment())
            .withMaxAppendInflightBytes(1024 * 1024 * 50)
            .withAppendRetryPolicy(AppendRetryPolicy.ALL)
            .build();

    final LinkedBlockingQueue<ListenableFuture<AppendOutput>> pendingAppends =
        new LinkedBlockingQueue<>();

    try (final var executor =
            MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(4));
        final var channel = ManagedChannelFactory.forBasinOrStreamService(config, basinName)) {

      final var consumer =
          executor.submit(
              () -> {
                try {
                  while (true) {
                    var output = pendingAppends.take().get();
                    if (output == null) {
                      logger.info("consumer closing");
                      break;
                    }
                    logger.info("consumer got: {}", output);
                  }
                } catch (Exception e) {
                  logger.error("consumer failed", e);
                }
              });

      final var streamClient =
          StreamClient.newBuilder(config, basinName, streamName)
              .withExecutor(executor)
              .withChannel(channel)
              .build();

      try (final var futureAppendSession = streamClient.managedAppendSession()) {

        for (var i = 0; i < 50_000; i++) {
          try {
            // Generate a record with approximately 10KiB of random text.
            var payload =
                RandomASCIIStringGenerator.generateRandomASCIIString(i + " - ", 1024 * 10);
            var append =
                futureAppendSession.submit(
                    AppendInput.newBuilder()
                        .withRecords(
                            List.of(
                                AppendRecord.newBuilder()
                                    .withBody(payload.getBytes(StandardCharsets.UTF_8))
                                    .build()))
                        .build(),
                    // Duration is how long we are willing to wait to receive a future.
                    Duration.ofSeconds(10));

            pendingAppends.add(append);
          } catch (RuntimeException e) {
            logger.error("producer failed", e);
            pendingAppends.add(Futures.immediateFailedFuture(e));
            break;
          }
        }

        logger.info("finished submitting all appends");

        // Signal to the consumer that no further appends are happening.
        pendingAppends.add(Futures.immediateFuture(null));
      }

      consumer.get();
    }
  }

  static class RandomASCIIStringGenerator {
    private static final String ASCII_PRINTABLE_CHARACTERS =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "abcdefghijklmnopqrstuvwxyz" + "0123456789";

    private static final Random RANDOM = new Random();

    public static String generateRandomASCIIString(String prefix, int length) {
      if (length < 0) {
        throw new IllegalArgumentException("Length cannot be negative.");
      }

      StringBuilder sb = new StringBuilder(length);
      sb.append(prefix);
      for (int i = 0; i < length - prefix.length(); i++) {
        int index = RANDOM.nextInt(ASCII_PRINTABLE_CHARACTERS.length());
        sb.append(ASCII_PRINTABLE_CHARACTERS.charAt(index));
      }
      return sb.toString();
    }
  }
}
